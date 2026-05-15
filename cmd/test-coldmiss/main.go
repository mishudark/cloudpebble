// Command test-coldmiss verifies cold-miss recovery from object storage.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
)

func main() {
	dir := filepath.Join(os.TempDir(), "cloudpebble-test-coldmiss")
	objDir := filepath.Join(os.TempDir(), "cloudpebble-test-coldmiss-obj")
	namespace := "test-coldmiss"

	_ = os.RemoveAll(dir)
	_ = os.RemoveAll(objDir)

	store, err := local.New(objDir)
	if err != nil {
		log.Fatal(err)
	}

	e, err := engine.Open(context.Background(), engine.Options{
		Dir:               dir,
		Store:             store,
		Namespace:         namespace,
		ColdMissThreshold: 2, // trigger recovery after 2 misses
		SyncInterval:      3600 * 1e9,
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = e.Set(context.Background(), []byte("foo"), []byte("bar"))
	_ = e.Sync(context.Background()) // upload to GCS

	// Verify normal read works
	v, err := e.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Before: foo = %s\n", v)

	// Delete local SST files to simulate data loss on local NVMe
	localFiles, _ := os.ReadDir(dir)
	for _, f := range localFiles {
		if f.Name() != "LOCK" {
			_ = os.Remove(filepath.Join(dir, f.Name()))
		}
	}
	fmt.Printf("Deleted %d local files (simulating cold node)\n", len(localFiles)-1)

	// First Get — miss
	_, err = e.Get([]byte("foo"))
	fmt.Printf("Get #1 (cold miss): err=%v\n", err)

	// Second Get — miss again, triggers recovery at threshold=2
	_, err = e.Get([]byte("foo"))
	fmt.Printf("Get #2 (cold miss): err=%v\n", err)

	// Wait for recovery to complete
	time.Sleep(2 * time.Second)

	// Third Get — should succeed after recovery
	v, err = e.Get([]byte("foo"))
	if err != nil {
		fmt.Printf("Get #3 FAIL: %v\n", err)
	} else {
		fmt.Printf("Get #3 (after recovery): foo = %s\n", v)
		fmt.Println("PASS: cold-miss recovery")
	}

	_ = e.Close()
}
