// Command test-eventual verifies eventual consistency mode using
// separate process runs to simulate crashes.
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
	if len(os.Args) < 2 {
		log.Fatal("usage: test-eventual <step1|step2>")
	}

	dir := filepath.Join(os.TempDir(), "cloudpebble-test-eventual")
	objDir := filepath.Join(os.TempDir(), "cloudpebble-test-eventual-obj")
	ns := "test-eventual"

	store, err := local.New(objDir)
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "step1":
		os.RemoveAll(dir)
		os.RemoveAll(objDir)
		store, err = local.New(objDir)
		if err != nil {
			log.Fatal(err)
		}
		e, err := engine.Open(engine.Options{
			Dir: dir, Store: store, Namespace: ns, SyncInterval: 3600 * 1e9,
		})
		if err != nil {
			log.Fatal(err)
		}
		e.Set(context.Background(), []byte("k1"), []byte("synced"))
		e.Sync(context.Background())

		// Write k2 WITHOUT syncing — only in GCS WAL
		e.Set(context.Background(), []byte("k2"), []byte("wal-only"))

		// Abrupt exit: no Close() call. Crash simulation.
		fmt.Println("step1 done: k1 synced, k2 in WAL only, crashed")

	case "step2":
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0755)

		// Debug: show WALs before opening
		wals, _ := store.List(context.Background(), ns+"/wal/")
		fmt.Printf("WAL files on disk: %d\n", len(wals))
		for _, w := range wals {
			fmt.Printf("  %s\n", w)
		}

		e, err := engine.Open(engine.Options{
			Dir:          dir,
			Store:        store,
			Namespace:    ns,
			Consistency:  engine.ConsistencyEventual,
			SyncInterval: 3600 * 1e9,
			ColdMissThreshold: 0,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer e.Close()

		v, err := e.Get([]byte("k1"))
		fmt.Printf("k1 = %q err=%v (should be from checkpoint)\n", v, err)

		v, err = e.Get([]byte("k2"))
		if err != nil {
			fmt.Println("k2 NOT FOUND (correct — WAL not replayed in eventual mode)")
		} else {
			fmt.Printf("k2 = %q (found immediately — from checkpoint?)\n", v)
		}

		// Wait for WAL replay loop
		fmt.Println("Waiting for WAL replay loop (5s)...")
		time.Sleep(8 * time.Second)

		v, err = e.Get([]byte("k2"))
		if err != nil {
			fmt.Printf("k2 STILL MISS: %v (FAIL)\n", err)
		} else {
			fmt.Printf("k2 = %q (self-healing via WAL replay)\n", v)
			fmt.Println("PASS: eventual consistency converges")
		}
	}
}
