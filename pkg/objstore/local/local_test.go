package local_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mishudark/cloudpebble/pkg/objstore"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"github.com/mishudark/cloudpebble/pkg/objstore/testutil"
)

func TestLocalContract(t *testing.T) {
	testutil.RunContractTests(t, func(tb testing.TB) objstore.Store {
		dir := t.TempDir()
		s, err := local.New(dir)
		if err != nil {
			tb.Fatal(err)
		}
		return s
	})
}

// TestPathTraversalPrevented verifies that the local store rejects paths
// that would escape the root directory via "..". Regression test for the
// path traversal vulnerability where filepath.Join resolved ".." without
// validation.
func TestPathTraversalPrevented(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	s, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Attempt to write outside root via path traversal.
	traversalPaths := []string{
		"../../etc/passwd",
		"../other-file",
		"sub/../../outside",
		"a/../../../tmp/escape",
	}
	for _, p := range traversalPaths {
		if putErr := s.Put(ctx, p, []byte("data")); putErr == nil {
			t.Fatalf("Put %q should have been rejected as path traversal", p)
		}
	}

	// Verify the files were NOT created outside root.
	for _, p := range traversalPaths {
		escaped := filepath.Join(root, filepath.FromSlash(p))
		cleaned := filepath.Clean(escaped)
		if _, statErr := os.Stat(cleaned); statErr == nil {
			rel, relErr := filepath.Rel(root, cleaned)
			if relErr != nil || len(rel) > 0 && rel[0] == '.' {
				t.Fatalf("path traversal succeeded: %q -> %q exists and is outside root", p, cleaned)
			}
		}
	}

	// Verify normal paths still work.
	if putErr := s.Put(ctx, "normal/file", []byte("safe")); putErr != nil {
		t.Fatalf("normal Put should succeed: %v", putErr)
	}
	data, err := s.Get(ctx, "normal/file")
	if err != nil {
		t.Fatalf("normal Get should succeed: %v", err)
	}
	if string(data) != "safe" {
		t.Fatalf("got %q, want %q", data, "safe")
	}
}
