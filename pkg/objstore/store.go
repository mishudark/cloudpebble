// Package objstore defines a pluggable interface for object storage backends
// (Google Cloud Storage, S3, Azure Blob, local filesystem, etc.).
//
// All methods are context-aware for cancellation/timeout support. Paths use
// forward-slash delimiters and are relative to a root implicitly defined by
// each backend's configuration (e.g. a GCS bucket prefix).
package objstore

import (
	"context"
	"io"
)

// Store is a minimal object storage interface. Backends implement Put, Get,
// Delete, List, and Exists against an implicit root (e.g. a GCS bucket or a
// local directory).
type Store interface {
	io.Closer

	// Put writes data to the object at the given path, creating or overwriting
	// it. The path is relative to the store's root.
	Put(ctx context.Context, path string, data []byte) error

	// Get reads the full contents of the object at path. Returns an error if the
	// object does not exist.
	Get(ctx context.Context, path string) ([]byte, error)

	// Delete removes the object at path. Deleting a non-existent object is not an
	// error.
	Delete(ctx context.Context, path string) error

	// List returns all object paths with the given prefix, sorted lexicographically.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists reports whether an object exists at path.
	Exists(ctx context.Context, path string) (bool, error)
}
