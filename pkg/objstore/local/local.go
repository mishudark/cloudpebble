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
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, fmt.Errorf("local: creating root %s: %w", root, err)
	}
	return &Store{root: root}, nil
}

func (s *Store) path(p string) (string, error) {
	full := filepath.Join(s.root, filepath.FromSlash(p))
	cleanedRoot := filepath.Clean(s.root)
	if !strings.HasPrefix(full, cleanedRoot+string(os.PathSeparator)) && full != cleanedRoot {
		return "", fmt.Errorf("local: path %q escapes root", p)
	}
	return full, nil
}

func (s *Store) Put(ctx context.Context, path string, data []byte) error {
	full, err := s.path(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0750); err != nil {
		return fmt.Errorf("local: mkdir for %s: %w", path, err)
	}
	if err := os.WriteFile(full, data, 0600); err != nil {
		return fmt.Errorf("local: writing %s: %w", path, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, path string) ([]byte, error) {
	full, err := s.path(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(full) //nolint:gosec // path validated by s.path()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("local: object %s not found: %w", path, objstore.ErrNotFound)
		}
		return nil, fmt.Errorf("local: reading %s: %w", path, err)
	}
	return data, nil
}

func (s *Store) Delete(ctx context.Context, path string) error {
	full, err := s.path(path)
	if err != nil {
		return err
	}
	err = os.Remove(full)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local: deleting %s: %w", path, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	searchDir, err := s.path(prefix)
	if err != nil {
		return nil, err
	}

	// If the search directory doesn't exist yet, return an empty list.
	// This is common for WAL directories before the first write.
	if _, statErr := os.Stat(searchDir); statErr != nil {
		if os.IsNotExist(statErr) {
			return nil, nil
		}
		return nil, fmt.Errorf("local: stat %s: %w", searchDir, statErr)
	}

	var found []string
	walkErr := filepath.WalkDir(searchDir, func(walkPath string, d os.DirEntry, walkPathErr error) error {
		if walkPathErr != nil {
			if os.IsPermission(walkPathErr) {
				return nil
			}
			return walkPathErr
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(s.root, walkPath)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		found = append(found, rel)
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("local: listing prefix %s: %w", prefix, walkErr)
	}
	sort.Strings(found)
	return found, nil
}

func (s *Store) Exists(ctx context.Context, path string) (bool, error) {
	full, err := s.path(path)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(full)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("local: checking exists %s: %w", path, err)
}

func (s *Store) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
	full, err := s.path(path)
	if err != nil {
		return objstore.ObjectInfo{}, err
	}
	fi, err := os.Stat(full)
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
	full, err := s.path(path)
	if err != nil {
		return err
	}
	if mkErr := os.MkdirAll(filepath.Dir(full), 0750); mkErr != nil {
		return fmt.Errorf("local: mkdir for %s: %w", path, mkErr)
	}
	f, err := os.Create(full) //nolint:gosec // path validated by s.path()
	if err != nil {
		return fmt.Errorf("local: creating %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("local: closing %s: %w", path, cerr)
		}
	}()
	var written int64
	written, err = io.CopyN(f, r, size)
	if err != nil && err != io.EOF {
		return fmt.Errorf("local: writing %s: %w", path, err)
	}
	if written != size {
		return fmt.Errorf("local: wrote %d bytes for %s, expected %d", written, path, size)
	}
	return nil
}

func (s *Store) GetReader(ctx context.Context, path string) (io.ReadCloser, error) {
	full, err := s.path(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(full) //nolint:gosec // path validated by s.path()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("local: object %s not found: %w", path, objstore.ErrNotFound)
		}
		return nil, fmt.Errorf("local: opening %s: %w", path, err)
	}
	return f, nil
}
