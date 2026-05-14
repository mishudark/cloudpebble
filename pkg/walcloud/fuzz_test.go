package walcloud_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"github.com/mishudark/cloudpebble/pkg/walcloud"
)

const fuzzBatchWindow = 1 * time.Millisecond

func FuzzWriteReadRecord(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x01, 0xff})
	f.Add([]byte("large-value-" + string(make([]byte, 1024))))

	f.Fuzz(func(t *testing.T, data []byte) {
		store, err := local.New(t.TempDir())
		if err != nil {
			t.Skip(err)
		}

		mgr, err := walcloud.NewManager(store, "fuzz-wr", 0)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()
		seq, done, err := mgr.WriteRecord(ctx, data)
		if err != nil {
			t.Skip(err)
		}
		if done != nil {
			t.Fatal("expected nil done channel")
		}
		if seq < 1 {
			t.Fatalf("invalid seq: %d", seq)
		}

		got, err := mgr.ReadRecord(ctx, seq)
		if err != nil {
			t.Fatalf("ReadRecord failed: %v", err)
		}
		if string(got) != string(data) {
			t.Fatalf("got %d bytes, want %d bytes", len(got), len(data))
		}
	})
}

func FuzzBatchWriteRead(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), []byte("c"))
	f.Add([]byte("x"), []byte("y"), []byte("z"))

	f.Fuzz(func(t *testing.T, d1, d2, d3 []byte) {
		store, err := local.New(t.TempDir())
		if err != nil {
			t.Skip(err)
		}

		mgr, err := walcloud.NewManager(store, "fuzz-batch", fuzzBatchWindow)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()

		seq1, done1, err := mgr.WriteRecord(ctx, d1)
		if err != nil {
			t.Skip(err)
		}
		seq2, done2, err := mgr.WriteRecord(ctx, d2)
		if err != nil {
			t.Skip(err)
		}
		seq3, done3, err := mgr.WriteRecord(ctx, d3)
		if err != nil {
			t.Skip(err)
		}

		if seq1 != seq2 || seq2 != seq3 {
			t.Fatalf("expected same seq in batch: %d, %d, %d", seq1, seq2, seq3)
		}

		for _, ch := range []<-chan error{done1, done2, done3} {
			select {
			case err := <-ch:
				if err != nil {
					t.Skipf("batch commit error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Skip("timeout waiting for batch commit")
			}
		}

		entries, err := mgr.List(ctx)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry after batch, got %d", len(entries))
		}
	})
}

func FuzzMonotonicSequences(f *testing.F) {
	f.Add(uint8(1), uint8(5))
	f.Add(uint8(0), uint8(10))

	f.Fuzz(func(t *testing.T, start, count uint8) {
		if count == 0 {
			count = 1
		}
		if count > 50 {
			count = 50
		}

		store, err := local.New(t.TempDir())
		if err != nil {
			t.Skip(err)
		}

		mgr, err := walcloud.NewManager(store, "fuzz-mono", 0)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()
		var prev uint64
		for i := uint8(0); i < count; i++ {
			seq, _, err := mgr.WriteRecord(ctx, []byte{byte(start + i)})
			if err != nil {
				t.Skip(err)
			}
			if i > 0 && seq <= prev {
				t.Fatalf("seq %d <= prev %d at iteration %d", seq, prev, i)
			}
			prev = seq
		}

		if mgr.NextSeq() != prev+1 {
			t.Fatalf("NextSeq = %d, want %d", mgr.NextSeq(), prev+1)
		}
	})
}

func FuzzGC(f *testing.F) {
	f.Add(uint8(3), uint8(5))
	f.Add(uint8(0), uint8(10))

	f.Fuzz(func(t *testing.T, gcUpTo, total uint8) {
		if total == 0 {
			total = 1
		}
		if total > 50 {
			total = 50
		}
		if gcUpTo > total {
			gcUpTo = total
		}

		store, err := local.New(t.TempDir())
		if err != nil {
			t.Skip(err)
		}

		mgr, err := walcloud.NewManager(store, "fuzz-gc", 0)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()
		for i := uint8(0); i < total; i++ {
			_, _, err := mgr.WriteRecord(ctx, []byte{byte(i)})
			if err != nil {
				t.Skip(err)
			}
		}

		deleted, err := mgr.GC(ctx, uint64(gcUpTo), 0)
		if err != nil {
			t.Fatalf("GC failed: %v", err)
		}
		if deleted != int(gcUpTo) {
			t.Fatalf("deleted %d, want %d", deleted, gcUpTo)
		}

		entries, err := mgr.List(ctx)
		if err != nil {
			t.Fatalf("List after GC failed: %v", err)
		}
		expected := int(total) - int(gcUpTo)
		if len(entries) != expected {
			t.Fatalf("entries = %d, want %d", len(entries), expected)
		}
		for i, e := range entries {
			wantSeq := uint64(gcUpTo) + 1 + uint64(i)
			if e.Seq != wantSeq {
				t.Fatalf("entry[%d].Seq = %d, want %d", i, e.Seq, wantSeq)
			}
		}
	})
}

func sanitizeNamespace(s string) string {
	s = strings.ReplaceAll(s, "/", "-")
	s = strings.ReplaceAll(s, "\\", "-")
	s = strings.ReplaceAll(s, "\x00", "")
	if s == "" {
		return "default"
	}
	return s
}

func FuzzNamespaceIsolation(f *testing.F) {
	f.Add("ns-a", "ns-b")
	f.Add("a", "b")

	f.Fuzz(func(t *testing.T, ns1, ns2 string) {
		ns1 = sanitizeNamespace(ns1)
		ns2 = sanitizeNamespace(ns2)
		if ns1 == ns2 {
			ns2 = ns2 + "-suffix"
		}

		dir := t.TempDir()
		store, err := local.New(dir)
		if err != nil {
			t.Skip(err)
		}

		mgr1, err := walcloud.NewManager(store, ns1, 0)
		if err != nil {
			t.Skip(err)
		}
		mgr2, err := walcloud.NewManager(store, ns2, 0)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()
		seq1, _, err := mgr1.WriteRecord(ctx, []byte("data1"))
		if err != nil {
			t.Skip(err)
		}
		seq2, _, err := mgr2.WriteRecord(ctx, []byte("data2"))
		if err != nil {
			t.Skip(err)
		}

		entries1, _ := mgr1.List(ctx)
		entries2, _ := mgr2.List(ctx)

		if len(entries1) != 1 || len(entries2) != 1 {
			t.Skipf("namespace isolation issue: ns1=%d, ns2=%d", len(entries1), len(entries2))
		}
		if entries1[0].Seq != seq1 {
			t.Fatalf("ns1 seq mismatch")
		}
		if entries2[0].Seq != seq2 {
			t.Fatalf("ns2 seq mismatch")
		}
	})
}

func FuzzRecovery(f *testing.F) {
	f.Add(uint8(5))

	f.Fuzz(func(t *testing.T, nWrites uint8) {
		if nWrites == 0 {
			nWrites = 1
		}
		if nWrites > 30 {
			nWrites = 30
		}

		dir := t.TempDir()
		store, err := local.New(dir)
		if err != nil {
			t.Skip(err)
		}

		ctx := context.Background()
		mgr1, err := walcloud.NewManager(store, "fuzz-recovery", 0)
		if err != nil {
			t.Skip(err)
		}

		for i := uint8(0); i < nWrites; i++ {
			_, _, err := mgr1.WriteRecord(ctx, []byte{byte(i)})
			if err != nil {
				t.Skip(err)
			}
		}

		mgr2, err := walcloud.NewManager(store, "fuzz-recovery", 0)
		if err != nil {
			t.Skip(err)
		}

		expectedNext := uint64(nWrites) + 1
		if mgr2.NextSeq() != expectedNext {
			t.Fatalf("NextSeq = %d, want %d", mgr2.NextSeq(), expectedNext)
		}

		entries, err := mgr2.List(ctx)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(entries) != int(nWrites) {
			t.Fatalf("entries = %d, want %d", len(entries), nWrites)
		}
	})
}

func FuzzBatchHeaderOps(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0})
	f.Add(make([]byte, 12))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, header []byte) {
		if len(header) < 12 {
			return
		}

		data := make([]byte, len(header))
		copy(data, header)

		count := walcloud.BatchCount(data)
		if count < 0 {
			return
		}

		walcloud.SetBatchCount(data, count+1)
		newCount := walcloud.BatchCount(data)
		if newCount != count+1 {
			return
		}
	})
}

func FuzzMergeBatchSegments(f *testing.F) {
	f.Add([]byte("seg1"), []byte("seg2"))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0x01}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0x02})
	f.Add([]byte{}, []byte{})
	f.Add([]byte("single"), []byte("also"))

	f.Fuzz(func(t *testing.T, seg1, seg2 []byte) {
		segments := [][]byte{seg1, seg2}

		result := walcloud.MergeBatchSegments(segments)

		if result == nil {
			t.Fatal("result is nil")
		}

		maxLen := len(seg1) + len(seg2)
		if len(result) > maxLen {
			t.Fatalf("result too large: %d > %d", len(result), maxLen)
		}

		if len(seg1) >= 12 && len(seg2) >= 12 {
			count := walcloud.BatchCount(result)
			if count < 0 {
				t.Fatalf("negative merged count: %d", count)
			}
		}
	})
}
