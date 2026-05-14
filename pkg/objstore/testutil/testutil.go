// Package testutil provides shared test helpers for objstore backends.
package testutil

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mishudark/cloudpebble/pkg/objstore"
)

// RunContractTests validates that a Store implementation satisfies the
// interface contract.
func RunContractTests(t *testing.T, newStore func(testing.TB) objstore.Store) {
	t.Helper()
	t.Run("PutGet", func(t *testing.T) { testPutGet(t, newStore(t)) })
	t.Run("PutOverwrite", func(t *testing.T) { testPutOverwrite(t, newStore(t)) })
	t.Run("GetNotFound", func(t *testing.T) { testGetNotFound(t, newStore(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, newStore(t)) })
	t.Run("DeleteNotFound", func(t *testing.T) { testDeleteNotFound(t, newStore(t)) })
	t.Run("List", func(t *testing.T) { testList(t, newStore(t)) })
	t.Run("ListEmptyPrefix", func(t *testing.T) { testListEmptyPrefix(t, newStore(t)) })
	t.Run("Exists", func(t *testing.T) { testExists(t, newStore(t)) })
	t.Run("Attrs", func(t *testing.T) { testAttrs(t, newStore(t)) })
	t.Run("AttrsNotFound", func(t *testing.T) { testAttrsNotFound(t, newStore(t)) })
	t.Run("EmptyPayload", func(t *testing.T) { testEmptyPayload(t, newStore(t)) })
	t.Run("LargePayload", func(t *testing.T) { testLargePayload(t, newStore(t)) })
}

func testPutGet(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	if err := s.Put(ctx, "key", []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("got %q, want %q", got, "value")
	}
}

func testPutOverwrite(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	s.Put(ctx, "key", []byte("first"))
	s.Put(ctx, "key", []byte("second"))
	got, _ := s.Get(ctx, "key")
	if string(got) != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

func testGetNotFound(t *testing.T, s objstore.Store) {
	_, err := s.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "NotFound") && !strings.Contains(err.Error(), "doesn't exist") {
		t.Fatalf("error should indicate not found, got: %v", err)
	}
}

func testDelete(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	s.Put(ctx, "key", []byte("value"))
	s.Delete(ctx, "key")
	ok, err := s.Exists(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("key should not exist after delete")
	}
}

func testDeleteNotFound(t *testing.T, s objstore.Store) {
	if err := s.Delete(context.Background(), "nonexistent"); err != nil {
		t.Fatal(err)
	}
}

func testList(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	files := []string{"dir/a", "dir/b", "dir/c", "other"}
	for _, f := range files {
		s.Put(ctx, f, []byte(f))
	}
	got, err := s.List(ctx, "dir/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "dir/a" || got[1] != "dir/b" || got[2] != "dir/c" {
		t.Fatalf("got %v", got)
	}
}

func testListEmptyPrefix(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	files := []string{"a", "b", "c"}
	for _, f := range files {
		s.Put(ctx, f, []byte(f))
	}
	got, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
}

func testExists(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	s.Put(ctx, "key", []byte("value"))
	ok, _ := s.Exists(ctx, "key")
	if !ok {
		t.Fatal("key should exist")
	}
	ok, _ = s.Exists(ctx, "nonexistent")
	if ok {
		t.Fatal("key should not exist")
	}
}

func testAttrs(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	s.Put(ctx, "key", []byte("hello"))
	info, err := s.Attrs(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 5 {
		t.Fatalf("size %d, want 5", info.Size)
	}
	if info.CreatedAt.IsZero() {
		t.Fatal("creation time is zero")
	}
	if time.Since(info.CreatedAt) > time.Minute {
		t.Fatalf("creation time too old: %v", info.CreatedAt)
	}
}

func testAttrsNotFound(t *testing.T, s objstore.Store) {
	_, err := s.Attrs(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func testEmptyPayload(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	s.Put(ctx, "empty", []byte{})
	got, _ := s.Get(ctx, "empty")
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func testLargePayload(t *testing.T, s objstore.Store) {
	ctx := context.Background()
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	s.Put(ctx, "large", payload)
	got, _ := s.Get(ctx, "large")
	if !bytes.Equal(payload, got) {
		t.Fatal("large payload mismatch")
	}
}
