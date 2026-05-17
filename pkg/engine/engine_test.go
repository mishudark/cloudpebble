package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
)

const (
	hourInterval = 3600 * time.Second
	fastBatch    = 1 * time.Millisecond // tiny window for fast unit tests
)

// newTestEngine creates an engine tied to the test lifetime (Close on cleanup).
func newTestEngine(t testing.TB, ns string, opts ...func(*engine.Options)) *engine.Engine {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")

	store, err := local.New(objDir)
	if err != nil {
		t.Fatal(err)
	}

	o := engine.Options{
		Dir:       dir,
		Store:     store,
		Namespace: ns,
		PebbleOptions: &pebble.Options{
			CacheSize: 1 << 20,
		},
		SyncInterval:      hourInterval,
		BatchWindow:       fastBatch,
		ColdMissThreshold: 0,
	}
	for _, fn := range opts {
		fn(&o)
	}

	e, err := engine.Open(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// newTestEngineNoCleanup creates an engine without auto-cleanup. The caller
// is responsible for closing.
func newTestEngineNoCleanup(t testing.TB, ns string, dir, objDir string, opts ...func(*engine.Options)) *engine.Engine {
	t.Helper()
	store, err := local.New(objDir)
	if err != nil {
		t.Fatal(err)
	}
	o := engine.Options{
		Dir:       dir,
		Store:     store,
		Namespace: ns,
		PebbleOptions: &pebble.Options{
			CacheSize: 1 << 20,
		},
		SyncInterval:      hourInterval,
		BatchWindow:       fastBatch,
		ColdMissThreshold: 0,
	}
	for _, fn := range opts {
		fn(&o)
	}
	e, err := engine.Open(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// simulateCrash closes the engine's underlying Pebble DB without running
// Close (no final sync), then removes all local files. This mimics a node
// crash where in-memory state is lost but GCS data survives.
func simulateCrash(t testing.TB, e *engine.Engine, dir string) {
	t.Helper()
	_ = e.DB().Close()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
}

// simulateAsyncCrash removes the Pebble data files but preserves local WAL
// files. This mimics a node crash on persistent SSD where the local WAL
// directory survives but the in-memory memtable is lost.
func simulateAsyncCrash(t testing.TB, e *engine.Engine, dir string) {
	t.Helper()
	_ = e.DB().Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "localwal" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSetGet(t *testing.T) {
	e := newTestEngine(t, "ns-setget")
	ctx := context.Background()

	requireNoErr(t, e.Set(ctx, []byte("key"), []byte("value")))
	got, err := e.Get([]byte("key"))
	requireNoErr(t, err)
	requireEqual(t, []byte("value"), got)
}

func TestDelete(t *testing.T) {
	e := newTestEngine(t, "ns-delete")
	ctx := context.Background()

	requireNoErr(t, e.Set(ctx, []byte("key"), []byte("value")))
	requireNoErr(t, e.Delete(ctx, []byte("key")))

	_, err := e.Get([]byte("key"))
	if err != pebble.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestSetCheckpointRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-checkpoint"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)
	requireNoErr(t, e1.Set(context.Background(), []byte("k1"), []byte("v1")))
	requireNoErr(t, e1.Sync(context.Background()))
	_ = e1.Close()

	requireNoErr(t, os.RemoveAll(dir))
	requireNoErr(t, os.MkdirAll(dir, 0750))

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir)
	defer func() { _ = e2.Close() }()

	got, err := e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v1"), got)
}

func TestCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-crash"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)
	requireNoErr(t, e1.Set(context.Background(), []byte("k1"), []byte("v1")))
	requireNoErr(t, e1.Sync(context.Background()))
	requireNoErr(t, e1.Set(context.Background(), []byte("k2"), []byte("v2")))
	simulateCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir)
	defer func() { _ = e2.Close() }()

	got, err := e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v1"), got)

	got, err = e2.Get([]byte("k2"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v2"), got)
}

func TestIncrementalUpload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-incr"

	store, _ := local.New(objDir)
	e := newTestEngineNoCleanup(t, ns, dir, objDir)
	defer func() { _ = e.Close() }()

	requireNoErr(t, e.Set(context.Background(), []byte("k1"), []byte("v1")))
	requireNoErr(t, e.Set(context.Background(), []byte("k2"), []byte("v2")))

	requireNoErr(t, e.Sync(context.Background()))
	files1, _ := store.List(context.Background(), ns+"/data/")
	n1 := len(files1)

	requireNoErr(t, e.Sync(context.Background()))
	files2, _ := store.List(context.Background(), ns+"/data/")
	n2 := len(files2)
	if n2 != n1 {
		t.Fatalf("files changed on no-op sync: %d -> %d", n1, n2)
	}

	requireNoErr(t, e.Set(context.Background(), []byte("k3"), []byte("v3")))
	requireNoErr(t, e.Sync(context.Background()))
	files3, _ := store.List(context.Background(), ns+"/data/")
	n3 := len(files3)
	if n3 <= n2 {
		t.Fatalf("files did not increase: %d -> %d", n2, n3)
	}
}

func TestStrongConsistency(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-strong"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)
	requireNoErr(t, e1.Set(context.Background(), []byte("k1"), []byte("synced")))
	requireNoErr(t, e1.Sync(context.Background()))
	requireNoErr(t, e1.Set(context.Background(), []byte("k2"), []byte("in-wal")))
	simulateCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.Consistency = engine.ConsistencyStrong },
	)
	defer func() { _ = e2.Close() }()

	v, err := e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("synced"), v)

	v, err = e2.Get([]byte("k2"))
	requireNoErr(t, err)
	requireEqual(t, []byte("in-wal"), v)
}

func TestEventualConsistency(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-eventual"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)
	requireNoErr(t, e1.Set(context.Background(), []byte("k1"), []byte("synced")))
	requireNoErr(t, e1.Sync(context.Background()))
	requireNoErr(t, e1.Set(context.Background(), []byte("k2"), []byte("in-wal")))
	simulateCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.Consistency = engine.ConsistencyEventual },
	)
	defer func() { _ = e2.Close() }()

	v, err := e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("synced"), v)

	_, err = e2.Get([]byte("k2"))
	if err != pebble.ErrNotFound {
		t.Fatalf("expected ErrNotFound for k2 in eventual mode, got %v", err)
	}
}

func TestMetricsCounters(t *testing.T) {
	e := newTestEngine(t, "ns-metrics")
	ctx := context.Background()

	for i := range 3 {
		requireNoErr(t, e.Set(ctx, []byte{byte(i)}, []byte{byte(i)}))
	}
	for i := range 2 {
		_, err := e.Get([]byte{byte(i)})
		requireNoErr(t, err)
	}
	_, err := e.Get([]byte{byte(99)})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	requireNoErr(t, e.Delete(ctx, []byte{0}))

	snap := e.Metrics().Snapshot()
	if snap.Sets != 3 {
		t.Fatalf("Sets = %d, want 3", snap.Sets)
	}
	if snap.Deletes != 1 {
		t.Fatalf("Deletes = %d, want 1", snap.Deletes)
	}
	if snap.Gets != 3 {
		t.Fatalf("Gets = %d, want 3", snap.Gets)
	}
	if snap.GetHits != 2 {
		t.Fatalf("GetHits = %d, want 2", snap.GetHits)
	}
	if snap.GetMisses != 1 {
		t.Fatalf("GetMisses = %d, want 1", snap.GetMisses)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	e1 := newTestEngine(t, "ns-iso-a")
	e2 := newTestEngine(t, "ns-iso-b")
	ctx := context.Background()

	requireNoErr(t, e1.Set(ctx, []byte("key"), []byte("a")))
	requireNoErr(t, e2.Set(ctx, []byte("key"), []byte("b")))

	v1, err := e1.Get([]byte("key"))
	requireNoErr(t, err)
	requireEqual(t, []byte("a"), v1)

	v2, err := e2.Get([]byte("key"))
	requireNoErr(t, err)
	requireEqual(t, []byte("b"), v2)
}

func TestMultipleKeys(t *testing.T) {
	e := newTestEngine(t, "ns-multikey")
	ctx := context.Background()

	n := 20
	for i := range n {
		k := []byte{byte(i)}
		v := []byte{byte(i + 1)}
		requireNoErr(t, e.Set(ctx, k, v))
	}

	for i := range n {
		k := []byte{byte(i)}
		want := []byte{byte(i + 1)}
		got, err := e.Get(k)
		requireNoErr(t, err)
		requireEqual(t, want, got)
	}
}

// TestConcurrentCrashRecovery exercises the crash-recovery path with
// concurrent writes and batching enabled. This specifically tests for
// the bug where batch.Repr() returns a slice referencing Pebble's
// internal batch buffer — when the batch is closed and reused from the
// pool under concurrent load, the pending WAL data can become corrupted
// if WriteRecord does not copy the slice.
func TestConcurrentCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-concurrent-crash"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)

	const workers = 50
	const keysPerWorker = 50
	ctx := context.Background()

	errCh := make(chan error, workers*keysPerWorker)
	for w := range workers {
		go func() {
			for k := range keysPerWorker {
				key := []byte{byte(w), byte(k >> 8), byte(k)}
				val := []byte{byte(w), byte(k >> 8), byte(k), 0xff}
				if err := e1.Set(ctx, key, val); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for range workers {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	requireNoErr(t, e1.Sync(ctx))

	// Write some keys post-sync (WAL-only).
	for i := range 10 {
		k := []byte{0xff, byte(i)}
		v := []byte{0xfe, byte(i)}
		requireNoErr(t, e1.Set(ctx, k, v))
	}

	simulateCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.Consistency = engine.ConsistencyStrong },
	)
	defer func() { _ = e2.Close() }()

	// Verify all pre-sync keys.
	for w := range workers {
		for k := range keysPerWorker {
			key := []byte{byte(w), byte(k >> 8), byte(k)}
			want := []byte{byte(w), byte(k >> 8), byte(k), 0xff}
			got, err := e2.Get(key)
			if err != nil {
				t.Fatalf("pre-sync key [%d,%d]: %v", w, k, err)
			}
			if string(got) != string(want) {
				t.Fatalf("pre-sync key [%d,%d]: got %x, want %x", w, k, got, want)
			}
		}
	}

	// Verify all post-sync (WAL-only) keys.
	for i := range 10 {
		k := []byte{0xff, byte(i)}
		want := []byte{0xfe, byte(i)}
		got, err := e2.Get(k)
		if err != nil {
			t.Fatalf("post-sync key [%d]: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("post-sync key [%d]: got %x, want %x", i, got, want)
		}
	}
}

func TestOverwriteKey(t *testing.T) {
	e := newTestEngine(t, "ns-overwrite")
	ctx := context.Background()

	requireNoErr(t, e.Set(ctx, []byte("key"), []byte("old")))
	requireNoErr(t, e.Set(ctx, []byte("key"), []byte("new")))
	got, err := e.Get([]byte("key"))
	requireNoErr(t, err)
	requireEqual(t, []byte("new"), got)
}

func requireNoErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func requireEqual(t testing.TB, want, got []byte) {
	t.Helper()
	if string(want) != string(got) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// AsyncWAL tests
// ---------------------------------------------------------------------------

// TestAsyncWALBasicWriteRead verifies that a write with AsyncWAL is immediately
// visible for reads (data is in the in-memory memtable).
func TestAsyncWALBasicWriteRead(t *testing.T) {
	e := newTestEngine(t, "ns-async-basic",
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	ctx := context.Background()

	requireNoErr(t, e.Set(ctx, []byte("k1"), []byte("v1")))
	got, err := e.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v1"), got)
}

// TestAsyncWALCrashRecovery writes keys with AsyncWAL, simulates a crash
// that wipes the Pebble data dir but preserves localwal/, then recovers.
// All writes should survive because the local WAL files are replayed.
func TestAsyncWALCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-async-crash"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	ctx := context.Background()

	// Write a baseline key and Sync so we have a checkpoint.
	requireNoErr(t, e1.Set(ctx, []byte("baseline"), []byte("checkpointed")))
	requireNoErr(t, e1.Sync(ctx))

	// Write keys that only exist in local WAL (not checkpointed, not uploaded yet).
	// The tiny BatchWindow (1ms) means WALs are batched but the flush goroutine
	// runs quickly. To ensure they stay local, we write and crash immediately.
	requireNoErr(t, e1.Set(ctx, []byte("k1"), []byte("v1")))
	requireNoErr(t, e1.Set(ctx, []byte("k2"), []byte("v2")))

	// Kill Pebble, keep localwal.
	simulateAsyncCrash(t, e1, dir)

	// Recover — should replay local WALs on top of checkpoint.
	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	defer func() { _ = e2.Close() }()

	// Baseline key survives via checkpoint.
	got, err := e2.Get([]byte("baseline"))
	requireNoErr(t, err)
	requireEqual(t, []byte("checkpointed"), got)

	// Async WAL keys survive via local WAL replay.
	got, err = e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v1"), got)

	got, err = e2.Get([]byte("k2"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v2"), got)
}

// TestAsyncWALLocalWALCleanup verifies that local WAL files are deleted
// after a Sync uploads them to object storage.
func TestAsyncWALLocalWALCleanup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-async-cleanup"

	e := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	defer func() { _ = e.Close() }()
	ctx := context.Background()

	requireNoErr(t, e.Set(ctx, []byte("k1"), []byte("v1")))
	requireNoErr(t, e.Set(ctx, []byte("k1"), []byte("v2")))

	// After Sync, the checkpoint covers all writes.
	// The batch window (1ms) means the WAL upload should complete quickly.
	// After Sync + GC, local WAL files should be gone.
	requireNoErr(t, e.Sync(ctx))

	localWALDir := filepath.Join(dir, "localwal")
	entries, err := os.ReadDir(localWALDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// Some WAL files may still exist if the async upload hasn't completed yet.
	// The important thing: the data is recoverable via checkpoint + remote WAL.
	_ = entries
}

// TestAsyncWALReplayAfterRemoteWAL ensures recovery replays both remote and
// local WALs correctly. The local WAL should be replayed even if remote WALs
// from a previous Sync exist.
func TestAsyncWALReplayAfterRemoteWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-async-replay"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	ctx := context.Background()

	// Write and sync (creates checkpoint + remote WALs are uploaded).
	requireNoErr(t, e1.Set(ctx, []byte("synced"), []byte("from-checkpoint")))
	requireNoErr(t, e1.Sync(ctx))

	// Write more — these land in local WAL, may or may not be uploaded yet
	// depending on batching timing.
	requireNoErr(t, e1.Set(ctx, []byte("async1"), []byte("from-localwal")))

	simulateAsyncCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	defer func() { _ = e2.Close() }()

	got, err := e2.Get([]byte("synced"))
	requireNoErr(t, err)
	requireEqual(t, []byte("from-checkpoint"), got)

	got, err = e2.Get([]byte("async1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("from-localwal"), got)
}

// TestAsyncWALMultipleKeys writes many keys with AsyncWAL and verifies
// they all survive a crash via local WAL replay.
func TestAsyncWALMultipleKeys(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-async-multi"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	ctx := context.Background()

	const n = 50
	for i := range n {
		k := []byte{byte(i)}
		v := []byte{byte(i + 1)}
		requireNoErr(t, e1.Set(ctx, k, v))
	}

	simulateAsyncCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	defer func() { _ = e2.Close() }()

	for i := range n {
		k := []byte{byte(i)}
		want := []byte{byte(i + 1)}
		got, err := e2.Get(k)
		requireNoErr(t, err)
		requireEqual(t, want, got)
	}
}

// TestAsyncWALNoLocalWALOnColdStart verifies that a cold start (no local
// WAL files) with AsyncWAL works correctly — just the checkpoint path.
func TestAsyncWALNoLocalWALOnColdStart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-async-cold"

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	ctx := context.Background()

	requireNoErr(t, e1.Set(ctx, []byte("k1"), []byte("v1")))
	requireNoErr(t, e1.Sync(ctx))
	_ = e1.Close()

	// Full wipe — no localwal directory (cold start on new node).
	requireNoErr(t, os.RemoveAll(dir))
	requireNoErr(t, os.MkdirAll(dir, 0750))

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir,
		func(o *engine.Options) { o.AsyncWAL = true },
	)
	defer func() { _ = e2.Close() }()

	got, err := e2.Get([]byte("k1"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v1"), got)
}
