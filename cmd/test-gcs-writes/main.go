// Command test-gcs-writes benchmarks CloudPebble write throughput against
// Google Cloud Storage, validates crash recovery by wiping local state and
// reading back all keys from GCS, then confirms the engine continues
// accepting writes after recovery.
//
// Usage (three phases):
//
//	export GCS_BUCKET=my-bucket
//	export GCS_PREFIX=cloudpebble-bench/   # optional
//
//	# Phase 1 — writes for 10s, then syncs + crashes
//	go run ./cmd/test-gcs-writes/ write
//
//	# Phase 2 — recovers from GCS, verifies keys, writes more, verifies again
//	go run ./cmd/test-gcs-writes/ recover
//
// Credentials are read via GOOGLE_APPLICATION_CREDENTIALS or ambient
// workload identity (ADC).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/gcs"
)

const (
	runDuration = 10 * time.Second
	concurrency = 50000
	valSize     = 200
)

// Durable marker files in GCS that survive a crash — used to pass state
// between write and recover invocations.
const (
	countMarker = "bench-count"
	keyFile     = "bench-sampled-keys"
	sampleRate  = 100 // store every Nth key to avoid giant manifest
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: test-gcs-writes <write|recover>")
	}

	bucket := os.Getenv("GCS_BUCKET")
	if bucket == "" {
		log.Fatal("GCS_BUCKET environment variable is required")
	}
	prefix := os.Getenv("GCS_PREFIX")
	if prefix == "" {
		prefix = "cloudpebble-bench/"
	}

	switch os.Args[1] {
	case "write":
		phaseWrite(bucket, prefix)
	case "recover":
		phaseRecover(bucket, prefix)
	default:
		log.Fatalf("unknown phase: %s (use 'write' or 'recover')", os.Args[1])
	}
}

func localDir() (string, error) {
	return os.MkdirTemp("", "cloudpebble-gcs-bench")
}

func phaseWrite(bucket, prefix string) {
	dir, err := localDir()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	store, err := gcs.New(bucket, prefix)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	e, err := engine.Open(context.Background(), engine.Options{
		Dir:               dir,
		Store:             store,
		Namespace:         "bench",
		SyncInterval:      1 * time.Hour,
		BatchWindow:       200 * time.Millisecond,
		ColdMissThreshold: 0,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	var totalWrites atomic.Int64
	var totalErrors atomic.Int64
	latencies := make([]int64, 0, 500000)
	var latMu spinLock

	value := make([]byte, valSize)
	for i := range value {
		value[i] = 'x'
	}

	fmt.Printf("=== CloudPebble GCS Write → Crash Recovery Test ===\n")
	fmt.Printf("Phase 1 — WRITE\n")
	fmt.Printf("Bucket:      gs://%s/%s\n", bucket, prefix)
	fmt.Printf("Duration:    %v\n", runDuration)
	fmt.Printf("Concurrency: %d goroutines\n", concurrency)
	fmt.Printf("Value size:  %d bytes\n", valSize)
	fmt.Printf("Local dir:   %s\n", filepath.Base(dir))
	fmt.Printf("--------------------------------------------------\n")

	deadline := time.Now().Add(runDuration)
	done := make(chan struct{})

	for i := range concurrency {
		go func(id int) {
			var localSeq int64
			for {
				if time.Now().After(deadline) {
					return
				}
				localSeq++
				key := fmt.Sprintf("goroutine-%d-key-%d", id, localSeq)

				start := time.Now()
				err := e.Set(ctx, []byte(key), value)
				elapsed := time.Since(start).Nanoseconds()

				if err != nil {
					totalErrors.Add(1)
					log.Printf("Set error (goroutine %d, key %s): %v", id, key, err)
					continue
				}
				totalWrites.Add(1)

				latMu.lock()
				latencies = append(latencies, elapsed/1e6)
				latMu.unlock()
			}
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				w := totalWrites.Load()
				elapsed := time.Since(deadline.Add(-runDuration))
				if elapsed > 0 {
					ops := float64(w) / elapsed.Seconds()
					fmt.Printf("  %5.0f ops/s | %d total | %d errors\n", ops, w, totalErrors.Load())
				}
			}
		}
	}()

	time.Sleep(runDuration)
	close(done)
	time.Sleep(1 * time.Second) // drain in-flight writes

	total := totalWrites.Load()
	errs := totalErrors.Load()
	ops := float64(total) / runDuration.Seconds()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Printf("Total writes:  %d\n", total)
	fmt.Printf("Errors:        %d\n", errs)
	fmt.Printf("Throughput:    %.0f ops/s\n", ops)

	if len(latencies) > 0 {
		slices.Sort(latencies)
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		fmt.Printf("P50 latency:   %d ms\n", p50)
		fmt.Printf("P95 latency:   %d ms\n", p95)
		fmt.Printf("P99 latency:   %d ms\n", p99)
		fmt.Printf("Samples:       %d\n", len(latencies))
	}
	fmt.Println("========================================")

	// --- Phase 1b: prepare for crash ---

	fmt.Println("\n--- Preparing for crash recovery ---")

	// Force a sync so part of the data is checkpointed (SSTs in GCS).
	fmt.Println("Syncing checkpoint to GCS...")
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	if err := e.Sync(syncCtx); err != nil {
		log.Printf("Sync error: %v", err)
	}
	syncCancel()

	// Write a few extra keys that will only exist in the WAL, not the
	// checkpoint. These test WAL replay on recovery.
	nPostSync := 5
	fmt.Printf("Writing %d keys post-sync (WAL-only)...\n", nPostSync)
	postCtx, postCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	for i := range nPostSync {
		key := fmt.Sprintf("post-sync-key-%d", i)
		if err := e.Set(postCtx, []byte(key), []byte("wal-only")); err != nil {
			log.Fatalf("post-sync Set: %v", err)
		}
	}
	postCancel()

	// Sample keys for the recovery phase to verify. We write the total
	// count and a representative sample to GCS so the recover phase knows
	// what to expect.
	syncCtx2, syncCancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
	defer syncCancel2()

	var sampled []string
	for gid := range concurrency {
		for j := int64(1); j <= 5; j++ {
			sampled = append(sampled, fmt.Sprintf("goroutine-%d-key-%d", gid, j))
		}
	}
	for i := range nPostSync {
		sampled = append(sampled, fmt.Sprintf("post-sync-key-%d", i))
	}
	sampledBytes := []byte(strings.Join(sampled, "\n"))
	if err := store.Put(syncCtx2, keyFile, sampledBytes); err != nil {
		log.Fatalf("writing key file: %v", err)
	}
	countBytes := []byte(strconv.FormatInt(total+int64(nPostSync), 10))
	if err := store.Put(syncCtx2, countMarker, countBytes); err != nil {
		log.Fatalf("writing count marker: %v", err)
	}

	fmt.Printf("Expected keys: %d\n", total+int64(nPostSync))
	fmt.Printf("Sample keys recorded: %d\n", len(sampled))

	// Simulate crash: close the Pebble DB without a clean Close().
	fmt.Println("Simulating crash (wiping local data)...")
	_ = e.DB().Close()
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nPhase 1 complete. Run 'test-gcs-writes recover' to validate recovery.")
}

func phaseRecover(bucket, prefix string) {
	dir, err := localDir()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	store, err := gcs.New(bucket, prefix)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	// Read expected count and sample keys from GCS.
	ctx := context.Background()

	countBytes, err := store.Get(ctx, countMarker)
	if err != nil {
		log.Fatalf("reading count marker: %v (did you run 'write' first?)", err)
	}
	expectedCount, err := strconv.ParseInt(string(countBytes), 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	keyBytes, err := store.Get(ctx, keyFile)
	if err != nil {
		log.Fatalf("reading key file: %v", err)
	}
	sampledKeys := strings.Split(string(keyBytes), "\n")

	fmt.Printf("=== CloudPebble GCS Write → Crash Recovery Test ===\n")
	fmt.Printf("Phase 2 — RECOVER\n")
	fmt.Printf("Bucket:       gs://%s/%s\n", bucket, prefix)
	fmt.Printf("Expected keys: %d\n", expectedCount)
	fmt.Printf("Sample keys:   %d\n", len(sampledKeys))
	fmt.Printf("Local dir:     %s\n", filepath.Base(dir))
	fmt.Printf("---------------------------------------------------\n")

	start := time.Now()
	e, err := engine.Open(context.Background(), engine.Options{
		Dir:               dir,
		Store:             store,
		Namespace:         "bench",
		SyncInterval:      1 * time.Hour,
		BatchWindow:       200 * time.Millisecond,
		ColdMissThreshold: 0,
		Consistency:       engine.ConsistencyStrong,
	})
	if err != nil {
		log.Fatalf("recovery Open: %v", err)
	}
	defer func() { _ = e.Close() }()

	recoverTime := time.Since(start)
	fmt.Printf("Recovery completed in %v\n", recoverTime)

	// Verify sampled keys.
	fmt.Println("\nVerifying sampled keys...")
	var verified, missing int
	for _, key := range sampledKeys {
		if key == "" {
			continue
		}
		_, err = e.Get([]byte(key))
		if err != nil {
			missing++
			if missing <= 10 {
				log.Printf("MISSING: %s — %v", key, err)
			}
		} else {
			verified++
		}
	}
	if missing > 10 {
		log.Printf("... and %d more missing", missing-10)
	}

	fmt.Println()
	fmt.Println("========================================")
	fmt.Printf("Recovery time:   %v\n", recoverTime)
	fmt.Printf("Sampled checked: %d\n", verified+missing)
	fmt.Printf("Verified:        %d\n", verified)
	fmt.Printf("Missing:         %d\n", missing)
	if missing == 0 {
		fmt.Println("Result:         PASS — all sampled keys recovered")
	} else {
		fmt.Println("Result:         FAIL — some keys not found")
	}
	fmt.Println("========================================")

	// --- Phase 3: post-recovery writes ---

	fmt.Println()
	fmt.Printf("=== Phase 3 — POST-RECOVERY WRITES ===\n")
	fmt.Printf("Writing for 5s at %d concurrency...\n", concurrency/10)

	ctx, cancel2 := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel2()

	var postWrites atomic.Int64
	var postErrors atomic.Int64

	v2 := make([]byte, valSize)
	for i := range v2 {
		v2[i] = 'y'
	}

	phase3Duration := 5 * time.Second
	deadline := time.Now().Add(phase3Duration)
	postDone := make(chan struct{})

	for i := range concurrency / 10 {
		go func(id int) {
			var localSeq int64
			for {
				if time.Now().After(deadline) {
					return
				}
				localSeq++
				key := fmt.Sprintf("recover-goroutine-%d-key-%d", id, localSeq)

				err = e.Set(ctx, []byte(key), v2)
				if err != nil {
					postErrors.Add(1)
					log.Printf("post-recovery Set error (goroutine %d, key %s): %v", id, key, err)
					continue
				}
				n := postWrites.Add(1)

				// Verify every 500th write inline.
				if n%500 == 0 {
					_, err = e.Get([]byte(key))
					if err != nil {
						log.Printf("post-recovery Get error (key %s): %v", key, err)
						postErrors.Add(1)
					}
				}
			}
		}(i)
	}

	ticker2 := time.NewTicker(1 * time.Second)
	defer ticker2.Stop()

	go func() {
		for {
			select {
			case <-postDone:
				return
			case <-ticker2.C:
				w := postWrites.Load()
				elapsed := time.Since(deadline.Add(-phase3Duration))
				if elapsed > 0 {
					ops := float64(w) / elapsed.Seconds()
					fmt.Printf("  post-recovery: %5.0f ops/s | %d total | %d errors\n", ops, w, postErrors.Load())
				}
			}
		}
	}()

	time.Sleep(phase3Duration)
	close(postDone)
	time.Sleep(1 * time.Second)

	postTotal := postWrites.Load()
	postErrs := postErrors.Load()
	postOps := float64(postTotal) / phase3Duration.Seconds()

	// Verify a handful of the post-recovery keys.
	var postVerified, postMissing int
	for gid := range concurrency / 10 {
		key := fmt.Sprintf("recover-goroutine-%d-key-%d", gid, int64(1))
		_, err = e.Get([]byte(key))
		if err != nil {
			postMissing++
			if postMissing <= 5 {
				log.Printf("post-missing: %s — %v", key, err)
			}
		} else {
			postVerified++
		}
	}

	fmt.Println()
	fmt.Println("========================================")
	fmt.Printf("Post-recovery writes: %d\n", postTotal)
	fmt.Printf("Post-recovery errors: %d\n", postErrs)
	fmt.Printf("Post-recovery throughput: %.0f ops/s\n", postOps)
	fmt.Printf("Post-recovery sampled: %d\n", postVerified+postMissing)
	fmt.Printf("Post-recovery verified: %d\n", postVerified)
	fmt.Printf("Post-recovery missing:  %d\n", postMissing)
	if postMissing == 0 {
		fmt.Println("Post-recovery result: PASS — engine writes + reads healthy after crash")
	} else {
		fmt.Println("Post-recovery result: FAIL — some post-recovery keys not found")
	}
	fmt.Println("========================================")

	fmt.Println()
	if err = e.Sync(ctx); err != nil {
		log.Printf("final sync: %v", err)
	}
	fmt.Println("Final sync complete.")

	// Clean up GCS objects from the benchmark.
	fmt.Println("\nCleaning up GCS objects...")
	cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cleanCancel()
	paths, err := store.List(cleanCtx, "")
	if err != nil {
		log.Printf("List error during cleanup: %v", err)
		return
	}
	for i, p := range paths {
		if i > 0 && i%100 == 0 {
			fmt.Printf("  deleted %d/%d objects...\n", i, len(paths))
		}
		if err := store.Delete(cleanCtx, p); err != nil {
			log.Printf("Delete error (%s): %v", p, err)
		}
	}
	fmt.Printf("Cleanup done: %d objects removed\n", len(paths))
}

type spinLock struct{ flag int32 }

func (s *spinLock) lock() {
	for {
		if atomic.CompareAndSwapInt32(&s.flag, 0, 1) {
			return
		}
		if s.flag == 0 {
			continue
		}
		time.Sleep(time.Microsecond)
	}
}

func (s *spinLock) unlock() { atomic.StoreInt32(&s.flag, 0) }

func init() {
	log.SetFlags(log.Ltime)
}
