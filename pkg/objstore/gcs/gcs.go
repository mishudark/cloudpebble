// Package gcs implements the objstore.Store interface backed by Google Cloud
// Storage.
package gcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/mishudark/cloudpebble/pkg/objstore"
)

var _ objstore.Store = (*Store)(nil)

type Store struct {
	client *storage.Client
	bucket string
	prefix string // optional prefix within the bucket to scope all operations
}

// New creates a new GCS-backed Store. The bucket parameter identifies the GCS
// bucket. The prefix, if non-empty, is prepended to all object paths, scoping
// the store to a subdirectory within the bucket. Additional client options
// (e.g. credentials, endpoint for emulator) may be passed via opts.
func New(bucket, prefix string, opts ...option.ClientOption) (*Store, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs: creating client: %w", err)
	}
	if prefix != "" {
		prefix = strings.TrimRight(prefix, "/") + "/"
	}
	return &Store{client: client, bucket: bucket, prefix: prefix}, nil
}

func (s *Store) fullPath(path string) string {
	return s.prefix + path
}

func (s *Store) unPrefix(fullPath string) string {
	return strings.TrimPrefix(fullPath, s.prefix)
}

func (s *Store) Put(ctx context.Context, path string, data []byte) error {
	w := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: writing %s: %w", path, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs: closing writer for %s: %w", path, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, path string) ([]byte, error) {
	r, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewReader(ctx)
	if err != nil {
		if isNotExist(err) {
			return nil, fmt.Errorf("gcs: object %s: %w", path, err)
		}
		return nil, fmt.Errorf("gcs: reading %s: %w", path, err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gcs: reading body %s: %w", path, err)
	}
	return data, nil
}

func (s *Store) Delete(ctx context.Context, path string) error {
	err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Delete(ctx)
	if err != nil && !isNotExist(err) {
		return fmt.Errorf("gcs: deleting %s: %w", path, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{
		Prefix: s.fullPath(prefix),
	})
	var out []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: listing prefix %s: %w", prefix, err)
		}
		out = append(out, s.unPrefix(attrs.Name))
	}
	return out, nil
}

func (s *Store) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Attrs(ctx)
	if err != nil {
		if isNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("gcs: checking exists %s: %w", path, err)
	}
	return true, nil
}

func (s *Store) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
	attrs, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Attrs(ctx)
	if err != nil {
		return objstore.ObjectInfo{}, err
	}
	return objstore.ObjectInfo{
		Path:      path,
		Size:      attrs.Size,
		CreatedAt: attrs.Created,
	}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) PutReader(ctx context.Context, path string, r io.Reader, size int64) error {
	w := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewWriter(ctx)
	w.Size = size
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: writing %s: %w", path, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs: closing writer for %s: %w", path, err)
	}
	return nil
}

func (s *Store) GetReader(ctx context.Context, path string) (io.ReadCloser, error) {
	r, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewReader(ctx)
	if err != nil {
		if isNotExist(err) {
			return nil, fmt.Errorf("gcs: object %s: %w", path, err)
		}
		return nil, fmt.Errorf("gcs: reading %s: %w", path, err)
	}
	return r, nil
}

func isNotExist(err error) bool {
	if err == storage.ErrObjectNotExist {
		return true
	}
	if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == http.StatusNotFound {
		return true
	}
	return false
}
