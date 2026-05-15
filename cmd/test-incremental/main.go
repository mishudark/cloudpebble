// Command test-incremental verifies that Sync() only uploads new files.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
)

func main() {
	dir := filepath.Join(os.TempDir(), "cloudpebble-test-incr")
	objDir := filepath.Join(os.TempDir(), "cloudpebble-test-incr-obj")
	namespace := "test-incr"

	_ = os.RemoveAll(dir)
	_ = os.RemoveAll(objDir)

	store, err := local.New(objDir)
	if err != nil {
		log.Fatal(err)
	}

	e, err := engine.Open(context.Background(), engine.Options{
		Dir:             dir,
		Store:           store,
		Namespace:       namespace,
		SyncInterval:    3600 * 1e9, // disable auto-sync during test
		ColdMissThreshold: 0,          // disable auto-recovery during test
	})
	if err != nil {
		log.Fatal(err)
	}

	// Write data and sync
	_ = e.Set(context.Background(), []byte("k1"), []byte("v1"))
	_ = e.Set(context.Background(), []byte("k2"), []byte("v2"))

	if err := e.Sync(context.Background()); err != nil {
		log.Fatal(err)
	}

	// Count files in object store after first sync
	files1, _ := store.List(context.Background(), "test-incr/data/")
	fmt.Printf("Files after Sync 1: %d\n", len(files1))

	// Sync again with no new data — should NOT re-upload any files
	if err := e.Sync(context.Background()); err != nil {
		log.Fatal(err)
	}
	files2, _ := store.List(context.Background(), "test-incr/data/")
	fmt.Printf("Files after Sync 2 (no new data): %d\n", len(files2))

	// Write new data and sync — should upload new files ONLY
	_ = e.Set(context.Background(), []byte("k3"), []byte("v3"))
	if err := e.Sync(context.Background()); err != nil {
		log.Fatal(err)
	}
	files3, _ := store.List(context.Background(), "test-incr/data/")
	fmt.Printf("Files after Sync 3 (new data): %d\n", len(files3))

	_ = e.Close()

	if len(files1) == len(files2) {
		fmt.Println("PASS: incremental upload — no redundant uploads on no-op sync")
	} else {
		fmt.Println("FAIL: files changed on no-op sync")
	}
	if len(files3) > len(files2) {
		fmt.Println("PASS: incremental upload — new files uploaded on data change")
	} else {
		fmt.Println("FAIL: no new files after data change")
	}
}
