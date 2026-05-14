package engine_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
)

func FuzzManifestParsing(f *testing.F) {
	f.Add([]byte(`{"version":1,"max_wal_seq":0,"created_at":"2024-01-01T00:00:00Z","prev_version":0,"files":[]}`))
	f.Add([]byte(`{"max_wal_seq":42}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"version":-1,"max_wal_seq":18446744073709551615}`))
	f.Add([]byte(`{"files":[{"name":"test.sst","size":100,"checksum":"abc123"}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var m engine.Manifest
		err := json.Unmarshal(data, &m)
		if err != nil {
			var old struct{ MaxWALSeq uint64 }
			if err2 := json.Unmarshal(data, &old); err2 != nil {
				return
			}
			m.MaxWALSeq = old.MaxWALSeq
		}

		if m.Version < 0 {
			return
		}
		for _, file := range m.Files {
			if file.Size < 0 {
				return
			}
		}

		encoded, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var m2 engine.Manifest
		if err := json.Unmarshal(encoded, &m2); err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if m2.MaxWALSeq != m.MaxWALSeq {
			t.Fatalf("round-trip mismatch: %d != %d", m2.MaxWALSeq, m.MaxWALSeq)
		}
	})
}

func FuzzSetGetDelete(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("a"), []byte("b"))
	f.Add([]byte("key-with-dashes"), []byte("value-with-dashes"))
	f.Add([]byte{0x00, 0x01, 0x02}, []byte{0x03, 0x04, 0x05})

	f.Fuzz(func(t *testing.T, key, value []byte) {
		dir := filepath.Join(t.TempDir(), "pebble")
		objDir := filepath.Join(t.TempDir(), "objstore")

		store, err := local.New(objDir)
		if err != nil {
			t.Skip(err)
		}

		e, err := engine.Open(context.Background(), engine.Options{
			Dir:               dir,
			Store:             store,
			Namespace:         "fuzz",
			SyncInterval:      hourInterval,
			BatchWindow:       fastBatch,
			ColdMissThreshold: 0,
		})
		if err != nil {
			t.Skip(err)
		}
		defer e.Close()

		ctx := context.Background()

		if err := e.Set(ctx, key, value); err != nil {
			t.Skip(err)
		}

		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("Get failed after Set: %v", err)
		}
		if string(got) != string(value) {
			t.Fatalf("got %q, want %q", got, value)
		}

		if err := e.Delete(ctx, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = e.Get(key)
		if err == nil {
			t.Fatal("expected error after delete")
		}
	})
}

func FuzzMultipleOperations(f *testing.F) {
	f.Add([]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"))
	f.Add([]byte("a"), []byte("b"), []byte("c"), []byte("d"))

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 []byte) {
		dir := filepath.Join(t.TempDir(), "pebble")
		objDir := filepath.Join(t.TempDir(), "objstore")

		store, err := local.New(objDir)
		if err != nil {
			t.Skip(err)
		}

		e, err := engine.Open(context.Background(), engine.Options{
			Dir:               dir,
			Store:             store,
			Namespace:         "fuzz-multi",
			SyncInterval:      hourInterval,
			BatchWindow:       fastBatch,
			ColdMissThreshold: 0,
		})
		if err != nil {
			t.Skip(err)
		}
		defer e.Close()

		ctx := context.Background()

		if err := e.Set(ctx, k1, v1); err != nil {
			t.Skip(err)
		}
		if err := e.Set(ctx, k2, v2); err != nil {
			t.Skip(err)
		}

		got1, err := e.Get(k1)
		if err != nil {
			t.Fatalf("Get k1 failed: %v", err)
		}
		if string(got1) != string(v1) {
			t.Fatalf("k1: got %q, want %q", got1, v1)
		}

		got2, err := e.Get(k2)
		if err != nil {
			t.Fatalf("Get k2 failed: %v", err)
		}
		if string(got2) != string(v2) {
			t.Fatalf("k2: got %q, want %q", got2, v2)
		}

		if err := e.Set(ctx, k1, v2); err != nil {
			t.Fatalf("overwrite failed: %v", err)
		}
		got1, err = e.Get(k1)
		if err != nil {
			t.Fatalf("Get after overwrite failed: %v", err)
		}
		if string(got1) != string(v2) {
			t.Fatalf("after overwrite: got %q, want %q", got1, v2)
		}
	})
}

func FuzzNamespacePaths(f *testing.F) {
	f.Add("ns")
	f.Add("namespace-with-dashes")
	f.Add("a")
	f.Add("ns/sub")
	f.Add("ns\\windows")

	f.Fuzz(func(t *testing.T, ns string) {
		if ns == "" {
			ns = "default"
		}

		dir := filepath.Join(t.TempDir(), "pebble")
		objDir := filepath.Join(t.TempDir(), "objstore")

		store, err := local.New(objDir)
		if err != nil {
			t.Skip(err)
		}

		e, err := engine.Open(context.Background(), engine.Options{
			Dir:               dir,
			Store:             store,
			Namespace:         ns,
			SyncInterval:      hourInterval,
			BatchWindow:       fastBatch,
			ColdMissThreshold: 0,
		})
		if err != nil {
			t.Skip(err)
		}
		defer e.Close()

		ctx := context.Background()
		key := []byte("test-key")
		value := []byte("test-value")

		if err := e.Set(ctx, key, value); err != nil {
			t.Skip(err)
		}

		got, err := e.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(got) != string(value) {
			t.Fatalf("got %q, want %q", got, value)
		}
	})
}

func FuzzMetricsSnapshot(f *testing.F) {
	f.Add([]byte("k"), []byte("v"))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		dir := filepath.Join(t.TempDir(), "pebble")
		objDir := filepath.Join(t.TempDir(), "objstore")

		store, err := local.New(objDir)
		if err != nil {
			t.Skip(err)
		}

		e, err := engine.Open(context.Background(), engine.Options{
			Dir:               dir,
			Store:             store,
			Namespace:         "fuzz-metrics",
			SyncInterval:      hourInterval,
			BatchWindow:       fastBatch,
			ColdMissThreshold: 0,
		})
		if err != nil {
			t.Skip(err)
		}
		defer e.Close()

		ctx := context.Background()
		_ = e.Set(ctx, key, value)
		_, _ = e.Get(key)
		_, _ = e.Get([]byte("nonexistent"))

		snap := e.Metrics().Snapshot()

		if snap.Sets < 0 {
			t.Fatalf("negative Sets: %d", snap.Sets)
		}
		if snap.Gets < 0 {
			t.Fatalf("negative Gets: %d", snap.Gets)
		}
		if snap.GetHits < 0 {
			t.Fatalf("negative GetHits: %d", snap.GetHits)
		}
		if snap.GetMisses < 0 {
			t.Fatalf("negative GetMisses: %d", snap.GetMisses)
		}
		if snap.GetHits+snap.GetMisses != snap.Gets {
			t.Fatalf("GetHits + GetMisses != Gets: %d + %d != %d", snap.GetHits, snap.GetMisses, snap.Gets)
		}
	})
}
