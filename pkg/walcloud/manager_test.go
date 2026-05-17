package walcloud_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"github.com/mishudark/cloudpebble/pkg/walcloud"
)

func newTestManager(t testing.TB, ns string, batchWindow time.Duration) *walcloud.Manager {
	t.Helper()
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := walcloud.NewManager(store, ns, batchWindow)
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func TestWriteAndRead(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	data := []byte("batch-data")
	seq, done, err := mgr.WriteRecord(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if done != nil {
		t.Fatal("expected nil done channel with no batching")
	}
	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}

	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Seq != 1 {
		t.Fatalf("entry seq = %d, want 1", entries[0].Seq)
	}

	read, err := mgr.ReadRecord(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(read) != string(data) {
		t.Fatalf("read %q, want %q", read, data)
	}
}

func TestSequenceNumbersMonotonic(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	var prev uint64
	for i := range 10 {
		seq, _, err := mgr.WriteRecord(ctx, []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
		if seq <= prev {
			t.Fatalf("seq %d <= prev %d", seq, prev)
		}
		prev = seq
	}
	if prev != 10 {
		t.Fatalf("last seq = %d, want 10", prev)
	}
}

func TestListOrdered(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	for i := range 5 {
		if _, _, err := mgr.WriteRecord(ctx, []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("entries = %d", len(entries))
	}
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entry[%d].Seq = %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestGC(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	for i := range 5 {
		if _, _, err := mgr.WriteRecord(ctx, []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	// GC up to seq 3
	deleted, err := mgr.GC(ctx, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("deleted %d, want 3", deleted)
	}

	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("remaining entries = %d, want 2", len(entries))
	}
	if entries[0].Seq != 4 || entries[1].Seq != 5 {
		t.Fatalf("seqs: %d, %d, want 4, 5", entries[0].Seq, entries[1].Seq)
	}
}

func TestGC_OrphanTTL(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	for i := range 3 {
		if _, _, err := mgr.WriteRecord(ctx, []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	// Small delay so created-at times are in the past.
	time.Sleep(50 * time.Millisecond)

	// GC with maxSeq=2. Seq 1,2 should be deleted normally.
	// Seq 3 (> maxSeq) should be deleted as orphan (TTL expired).
	deleted, err := mgr.GC(ctx, 2, 1*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("deleted %d, want 3", deleted)
	}

	entries, _ := mgr.List(ctx)
	if len(entries) != 0 {
		t.Fatalf("expected all entries GC'd, got %d", len(entries))
	}
}

func TestRecoveryFromExisting(t *testing.T) {
	dir := t.TempDir()
	store, err := local.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create first manager, write some WALs.
	mgr1, _ := walcloud.NewManager(store, "ns", 0)
	ctx := context.Background()
	for range 3 {
		if _, _, writeErr := mgr1.WriteRecord(ctx, []byte("x")); writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	// Create second manager (same store); nextSeq should be 4.
	mgr2, err := walcloud.NewManager(store, "ns", 0)
	if err != nil {
		t.Fatal(err)
	}
	if mgr2.NextSeq() != 4 {
		t.Fatalf("nextSeq = %d, want 4", mgr2.NextSeq())
	}
}

func TestBatching_SameSeq(t *testing.T) {
	mgr := newTestManager(t, "ns", 200*time.Millisecond)
	ctx := context.Background()

	// First write opens a batch window.
	seq1, done1, err := mgr.WriteRecord(ctx, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if done1 == nil {
		t.Fatal("expected done channel with batching")
	}

	// Second write within the window should share the same seq.
	seq2, done2, err := mgr.WriteRecord(ctx, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if done2 == nil {
		t.Fatal("expected done channel with batching")
	}
	if seq1 != seq2 {
		t.Fatalf("seq1=%d, seq2=%d, want same seq", seq1, seq2)
	}
}

func TestBatching_TimerCommits(t *testing.T) {
	mgr := newTestManager(t, "ns", 100*time.Millisecond)
	ctx := context.Background()

	_, done, err := mgr.WriteRecord(ctx, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the timer to fire.
	select {
	case gcsErr := <-done:
		if gcsErr != nil {
			t.Fatalf("commit error: %v", gcsErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for batch commit")
	}

	// After commit, the WAL should exist in the store.
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
}

func TestNoBatching(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	ctx := context.Background()

	for i := range 3 {
		seq, done, err := mgr.WriteRecord(ctx, []byte("data"))
		if err != nil {
			t.Fatal(err)
		}
		if done != nil {
			t.Fatal("expected nil done channel with no batching")
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}

	entries, _ := mgr.List(ctx)
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
}

// TestBatching_CopiesData verifies that WriteRecord copies the data slice
// instead of retaining a reference. The bug: batch.Repr() returns a slice
// referencing Pebble's internal batch buffer; when the batch is closed and
// the buffer is reused, the pending WAL data becomes corrupted.
func TestBatching_CopiesData(t *testing.T) {
	mgr := newTestManager(t, "ns", 100*time.Millisecond)
	ctx := context.Background()

	original := []byte("hello-world-batch-repr-data")
	_, done, err := mgr.WriteRecord(ctx, original)
	if err != nil {
		t.Fatal(err)
	}

	// Modify the original slice. If WriteRecord didn't copy, the pending
	// data is now corrupted.
	for i := range original {
		original[i] = 'X'
	}

	// Flush to commit the pending batch to the store.
	err = mgr.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Read back and verify it's the original data.
	read, err := mgr.ReadRecord(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := "hello-world-batch-repr-data"
	if string(read) != want {
		t.Fatalf("ReadRecord after Flush = %q, want %q (WriteRecord did not copy data)", read, want)
	}

	// Also verify via the done channel (timer path).
	select {
	case gcsErr := <-done:
		if gcsErr != nil {
			t.Fatalf("commit error: %v", gcsErr)
		}
	default:
		// Timer may have already fired before Flush; that's fine.
	}
}

// TestMergeBatchSegmentsDataIntegrity verifies that mergeBatchSegments correctly
// handles segments of various lengths, including edge cases where segments are
// shorter than batchHeaderLen (12 bytes). Regression test for the bug where
// segments of length 8-11 were counted in the size pass but silently skipped
// in the data-copy pass, producing zero-filled corrupt output.
func TestMergeBatchSegmentsDataIntegrity(t *testing.T) {
	t.Parallel()

	// Build valid batch segments where each segment contains real batch repr data.
	// A Pebble batch repr has: 8-byte seqnum + 4-byte count + records.
	// We need segments >= batchHeaderLen for the "allValid" fast path.
	makeSegment := func(data []byte) []byte {
		// 12-byte header + data.
		buf := make([]byte, 12+len(data))
		walcloud.SetBatchCount(buf, 1)
		copy(buf[12:], data)
		return buf
	}

	// Segment that is shorter than batchHeaderLen (triggers the fallback path).
	shortSeg := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}

	// Two valid segments with different data.
	seg1 := makeSegment([]byte("hello"))
	seg2 := makeSegment([]byte("world"))

	// Test 1: valid + short segment → fallback to raw concatenation.
	result1 := walcloud.MergeBatchSegments([][]byte{seg1, shortSeg})
	if len(result1) != len(seg1)+len(shortSeg) {
		t.Fatalf("test1: len=%d, want %d", len(result1), len(seg1)+len(shortSeg))
	}
	// Verify both data payloads are present.
	if !bytes.Contains(result1, []byte("hello")) {
		t.Fatal("test1: result missing 'hello'")
	}
	if !bytes.Contains(result1, shortSeg) {
		t.Fatal("test1: result missing shortSeg content")
	}

	// Test 2: two valid segments → fast path with header stripping.
	result2 := walcloud.MergeBatchSegments([][]byte{seg1, seg2})
	want2 := len(seg1) + len(seg2) - 12 // one header stripped
	if len(result2) != want2 {
		t.Fatalf("test2: len=%d, want %d", len(result2), want2)
	}
	if !bytes.Contains(result2, []byte("hello")) {
		t.Fatal("test2: result missing 'hello'")
	}
	if !bytes.Contains(result2, []byte("world")) {
		t.Fatal("test2: result missing 'world'")
	}
	if walcloud.BatchCount(result2) != 2 {
		t.Fatalf("test2: batchCount=%d, want 2", walcloud.BatchCount(result2))
	}

	// Test 3: short + short + valid → all fallback.
	result3 := walcloud.MergeBatchSegments([][]byte{shortSeg, []byte{0x01}, seg1})
	want3 := len(shortSeg) + 1 + len(seg1)
	if len(result3) != want3 {
		t.Fatalf("test3: len=%d, want %d", len(result3), want3)
	}
	if !bytes.Contains(result3, shortSeg) {
		t.Fatal("test3: result missing shortSeg")
	}
	if !bytes.Contains(result3, []byte{0x01}) {
		t.Fatal("test3: result missing 0x01")
	}
	if !bytes.Contains(result3, []byte("hello")) {
		t.Fatal("test3: result missing 'hello'")
	}

	// Test 4: single segment (no merge needed).
	result4 := walcloud.MergeBatchSegments([][]byte{seg1})
	if len(result4) != len(seg1) {
		t.Fatalf("test4: len=%d, want %d", len(result4), len(seg1))
	}

	// Test 5: empty.
	result5 := walcloud.MergeBatchSegments(nil)
	if result5 != nil {
		t.Fatal("test5: expected nil")
	}
}

func TestNextSeq(t *testing.T) {
	mgr := newTestManager(t, "ns", 0)
	if mgr.NextSeq() != 1 {
		t.Fatalf("initial NextSeq = %d, want 1", mgr.NextSeq())
	}
	if _, _, err := mgr.WriteRecord(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if mgr.NextSeq() != 2 {
		t.Fatalf("NextSeq after write = %d, want 2", mgr.NextSeq())
	}
}
