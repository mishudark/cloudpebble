// Command cloudpebble is the CLI tool for testing CloudPebble.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
)

func main() {
	dir := "testdata/cloudpebble"
	objDir := "testdata/objectstore"
	namespace := "default"

	store, err := local.New(objDir)
	if err != nil {
		log.Fatal(err)
	}

	e, err := engine.Open(context.Background(), engine.Options{
		Dir:       dir,
		Store:     store,
		Namespace: namespace,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	key := []byte("hello")
	value := []byte("world")

	fmt.Printf("Setting %q = %q\n", key, value)
	if err := e.Set(ctx, key, value); err != nil {
		log.Fatal(err)
	}

	got, err := e.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got %q = %q\n", key, got)

	fmt.Println("OK: cloudpebble is working")
}
