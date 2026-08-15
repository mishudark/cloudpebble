package engine_test

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/engine"
)

// applyMerge writes one int64-sum merge operand through the engine's durable
// write path.
func applyMerge(t *testing.T, e *engine.Engine, key []byte, delta int64) {
	t.Helper()
	batch := e.DB().NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Merge(key, engine.EncodeInt64SumOperand(delta), nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
}

func readSum(t *testing.T, e *engine.Engine, key []byte) int64 {
	t.Helper()
	val, err := e.Get(key)
	requireNoErr(t, err)
	if len(val) != 8 {
		t.Fatalf("expected 8-byte sum, got %d bytes", len(val))
	}
	return int64(binary.BigEndian.Uint64(val))
}

// TestInt64SumMerger verifies merge operands accumulate across batches,
// memtable flushes, and compactions.
func TestInt64SumMerger(t *testing.T) {
	e := newTestEngine(t, "merge")
	key := []byte("counter")

	applyMerge(t, e, key, 5)
	applyMerge(t, e, key, 3)
	if got := readSum(t, e, key); got != 8 {
		t.Fatalf("memtable: expected 8, got %d", got)
	}

	// Flush one operand to an SST, then add another so the sum spans the
	// memtable and an SSTable.
	requireNoErr(t, e.DB().Flush())
	applyMerge(t, e, key, 4)
	if got := readSum(t, e, key); got != 12 {
		t.Fatalf("memtable+sst: expected 12, got %d", got)
	}

	// Compact everything into a single SSTable; the merged total must survive.
	requireNoErr(t, e.DB().Flush())
	requireNoErr(t, e.DB().Compact(context.Background(), []byte("a"), []byte("z"), false))
	if got := readSum(t, e, key); got != 12 {
		t.Fatalf("compacted: expected 12, got %d", got)
	}
}

// TestInt64SumMergerSetBase verifies a Set value acts as the base for later
// merge operands (SetCell initializes a counter, adds accumulate on top).
func TestInt64SumMergerSetBase(t *testing.T) {
	e := newTestEngine(t, "merge-set")
	key := []byte("counter")

	requireNoErr(t, e.Set(context.Background(), key, engine.EncodeInt64SumOperand(100)))
	applyMerge(t, e, key, 23)
	if got := readSum(t, e, key); got != 123 {
		t.Fatalf("expected 123, got %d", got)
	}
}

// TestInt64SumMergerCrashRecovery verifies merge operands survive a crash:
// they are replayed from the object-storage WAL and merged on recovery.
func TestInt64SumMergerCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")
	ns := "ns-merge-crash"
	key := []byte("counter")

	e1 := newTestEngineNoCleanup(t, ns, dir, objDir)
	applyMerge(t, e1, key, 5)
	applyMerge(t, e1, key, 3)
	simulateCrash(t, e1, dir)

	e2 := newTestEngineNoCleanup(t, ns, dir, objDir)
	defer func() { _ = e2.Close() }()

	if got := readSum(t, e2, key); got != 8 {
		t.Fatalf("after recovery: expected 8, got %d", got)
	}
}

// TestLegacyMergerFallback verifies an engine can still open a local store
// created with Pebble's concatenative default merger (pre-aggregate
// releases), with aggregate support disabled rather than a hard failure.
func TestLegacyMergerFallback(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")

	// Create a legacy store with Pebble's default merger.
	legacy, err := pebble.Open(dir, &pebble.Options{DisableWAL: true})
	requireNoErr(t, err)
	requireNoErr(t, legacy.Set([]byte("k"), []byte("v"), pebble.NoSync))
	requireNoErr(t, legacy.Flush()) // persist before close; WAL is disabled
	requireNoErr(t, legacy.Close())

	e := newTestEngine(t, "ns-legacy", func(o *engine.Options) {
		o.Dir = dir
	})
	if e.AggregatesSupported() {
		t.Fatal("expected aggregates to be disabled on a legacy store")
	}
	got, err := e.Get([]byte("k"))
	requireNoErr(t, err)
	requireEqual(t, []byte("v"), got)
}
