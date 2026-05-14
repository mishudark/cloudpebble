// Command test-recovery verifies crash recovery from GCS WAL.
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
	if len(os.Args) < 2 {
		log.Fatal("usage: test-recovery <step1|step2>")
	}

	dir := filepath.Join(os.TempDir(), "cloudpebble-test-recovery")
	objDir := filepath.Join(os.TempDir(), "cloudpebble-test-recovery-obj")
	namespace := "test"

	switch os.Args[1] {
	case "step1":
		os.RemoveAll(dir)
		os.RemoveAll(objDir)
		os.MkdirAll(dir, 0755)

		store, err := local.New(objDir)
		if err != nil {
			log.Fatal(err)
		}

		e, err := engine.Open(engine.Options{
			Dir:       dir,
			Store:     store,
			Namespace: namespace,
		})
		if err != nil {
			log.Fatal(err)
		}

		if err := e.Set(context.Background(), []byte("k1"), []byte("v1")); err != nil {
			log.Fatal(err)
		}
		if err := e.Set(context.Background(), []byte("k2"), []byte("v2")); err != nil {
			log.Fatal(err)
		}

		// Force sync to upload SSTs and GC WALs
		if err := e.Sync(context.Background()); err != nil {
			log.Fatal(err)
		}

		// Write more data WITHOUT syncing (these are only in GCS WAL)
		if err := e.Set(context.Background(), []byte("k3"), []byte("v3")); err != nil {
			log.Fatal(err)
		}

		fmt.Println("step1 done: k1,k2 synced, k3 only in WAL")
		// Intentionally don't call Close() to simulate crash

	case "step2":
		// Delete local data to simulate node restart / cold recovery
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0755)

		store, err := local.New(objDir)
		if err != nil {
			log.Fatal(err)
		}

		e, err := engine.Open(engine.Options{
			Dir:       dir,
			Store:     store,
			Namespace: namespace,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer e.Close()

		for _, k := range []string{"k1", "k2", "k3"} {
			v, err := e.Get([]byte(k))
			if err != nil {
				log.Fatalf("%s: %v", k, err)
			}
			fmt.Printf("%s = %s\n", k, v)
		}

		fmt.Println("recovery OK: k1,k2 from SSTs, k3 from WAL replay")
	}
}
