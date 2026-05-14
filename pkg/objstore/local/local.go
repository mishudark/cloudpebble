// Package local implements the objstore.Store interface backed by the local
// filesystem. It is intended for development and testing.
package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mishudark/cloudpebble/pkg/objstore"
)

var _ objstore.Store = (*Store)(nil)

type Store struct {
	root string
}

// New creates a new local FS-backed Store rooted at the given directory.
// The directory is created if it does not exist.
func New(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("local: creating root %s: %w", root, err)
	}
	return &Store{root: root}, nil
}

func (s *Store) path(p string) string {
	return filepath.Join(s.root, filepath.FromSlash(p))
}

func (s *Store) Put(ctx context.Context, path string, data []byte) error {
	full := s.path(path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return fmt.Errorf("local: mkdir for %s: %w", path, err)
	}
	if err := os.WriteFile(full, data, 0644); err != nil {
		return fmt.Errorf("local: writing %s: %w", path, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, path string) ([]byte, error) {
	data, err := os.ReadFile(s.path(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("local: object %s not found: %w", path, err)
		}
		return nil, fmt.Errorf("local: reading %s: %w", path, err)
	}
	return data, nil
}

func (s *Store) Delete(ctx context.Context, path string) error {
	err := os.Remove(s.path(path))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local: deleting %s: %w", path, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var found []string
	searchDir := s.path(prefix)
	prefixDir := filepath.Dir(searchDir)
	prefixBase := filepath.FromSlash(prefix)

	err := filepath.Walk(s.root, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, walkPath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefixBase) {
			found = append(found, rel)
		}
		return nil
	})
	_ = prefixDir // used above for prefix dir
	if err != nil {
		return nil, fmt.Errorf("local: listing prefix %s: %w", prefix, err)
	}
	sort.Strings(found)
	return found, nil
}

func (s *Store) Exists(ctx context.Context, path string) (bool, error) {
	_, err := os.Stat(s.path(path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("local: checking exists %s: %w", path, err)
}

func (s *Store) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
	fi, err := os.Stat(s.path(path))
	if err != nil {
		return objstore.ObjectInfo{}, err
	}
	return objstore.ObjectInfo{
		Path:      path,
		Size:      fi.Size(),
		CreatedAt: fi.ModTime(),
	}, nil
}

func (s *Store) Close() error { return nil }

func (s *Store) PutReader(ctx context.Context, path string, r io.Reader, size int64) error {
	full := s.path(path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return fmt.Errorf("local: mkdir for %s: %w", path, err)
	}
	f, err := os.Create(full)
	if err != nil {
		return fmt.Errorf("local: creating %s: %w", path, err)
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("local: writing %s: %w", path, err)
	}
	return nil
}

func (s *Store) GetReader(ctx context.Context, path string) (io.ReadCloser, error) {
	f, err := os.Open(s.path(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("local: object %s not found: %w", path, err)
		}
		return nil, fmt.Errorf("local: opening %s: %w", path, err)
	}
	return f, nil
}
