package gcs_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/iterator"
	"github.com/mishudark/cloudpebble/pkg/objstore"
	"github.com/mishudark/cloudpebble/pkg/objstore/testutil"
)

type fakeStore struct {
	client *storage.Client
	bucket string
	prefix string
}

func (s *fakeStore) Close() error {
	return s.client.Close()
}

func (s *fakeStore) fullPath(path string) string {
	return s.prefix + path
}

func (s *fakeStore) Put(ctx context.Context, path string, data []byte) error {
	w := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (s *fakeStore) Get(ctx context.Context, path string) ([]byte, error) {
	r, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func (s *fakeStore) Delete(ctx context.Context, path string) error {
	err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Delete(ctx)
	if err != nil && errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return err
}

func (s *fakeStore) List(ctx context.Context, prefix string) ([]string, error) {
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: s.fullPath(prefix)})
	var out []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, attrs.Name[len(s.prefix):])
	}
	return out, nil
}

func (s *fakeStore) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *fakeStore) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
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

func (s *fakeStore) PutReader(ctx context.Context, path string, r io.Reader, size int64) error {
	w := s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewWriter(ctx)
	w.Size = size
	if 	_, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (s *fakeStore) GetReader(ctx context.Context, path string) (io.ReadCloser, error) {
	return s.client.Bucket(s.bucket).Object(s.fullPath(path)).NewReader(ctx)
}

func newTestStore(t testing.TB) objstore.Store {
	t.Helper()

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Host: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)

	server.CreateBucket("test-bucket") //nolint:staticcheck

	return &fakeStore{
		client: server.Client(),
		bucket: "test-bucket",
		prefix: "test-prefix/",
	}
}

func TestGCSContract(t *testing.T) {
	testutil.RunContractTests(t, newTestStore)
}

func TestGCSStreaming(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	content := []byte("streaming test content")
	reader := &byteReader{data: content}

	if err := store.PutReader(ctx, "stream-key", reader, int64(len(content))); err != nil {
		t.Fatalf("PutReader: %v", err)
	}

	got, err := store.Get(ctx, "stream-key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("got %q, want %q", got, content)
	}

	rc, err := store.GetReader(ctx, "stream-key")
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	defer func() { _ = rc.Close() }()

	buf := make([]byte, len(content))
	n, err := io.ReadFull(rc, buf)
	if err != nil && n != len(content) {
		t.Fatalf("Read: got %d bytes, want %d, err=%v", n, len(content), err)
	}
	if string(buf) != string(content) {
		t.Fatalf("streaming read got %q, want %q", buf, content)
	}
}

func TestGCSStreamingLarge(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	payload := make([]byte, 5<<20) // 5MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	if err := store.PutReader(ctx, "large-stream", &byteReader{data: payload}, int64(len(payload))); err != nil {
		t.Fatalf("PutReader large: %v", err)
	}

	rc, err := store.GetReader(ctx, "large-stream")
	if err != nil {
		t.Fatalf("GetReader large: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatal(err)
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("byte %d mismatch: got %d, want %d", i, got[i], payload[i])
		}
	}
}

type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestGCSNamespaceIsolation(t *testing.T) {
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Host: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	server.CreateBucket("test-bucket") //nolint:staticcheck

	client := server.Client()
	defer func() { _ = client.Close() }()

	storeA := &fakeStore{client: client, bucket: "test-bucket", prefix: "ns-a/"}
	storeB := &fakeStore{client: client, bucket: "test-bucket", prefix: "ns-b/"}

	ctx := context.Background()

	_ = storeA.Put(ctx, "key", []byte("from-a"))
	_ = storeB.Put(ctx, "key", []byte("from-b"))

	gotA, _ := storeA.Get(ctx, "key")
	gotB, _ := storeB.Get(ctx, "key")

	if string(gotA) != "from-a" {
		t.Fatalf("storeA got %q, want from-a", gotA)
	}
	if string(gotB) != "from-b" {
		t.Fatalf("storeB got %q, want from-b", gotB)
	}
}
