// Package walcloud manages a write-ahead log stored in object storage.
// Each write produces an immutable WAL object. The local Pebble instance
// replays these WAL objects into its memtable for fast reads.
//
// WAL object paths follow the convention:
//
//	{namespace}/wal/{seqnum}.wal
//
// where seqnum is a zero-padded 20-digit monotonic counter.
package walcloud

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mishudark/cloudpebble/pkg/objstore"
)

const (
	walDir      = "wal"
	walExt      = ".wal"
	seqNumWidth = 20
)

// Manager coordinates the lifecycle of WAL objects in object storage.
type Manager struct {
	store objstore.Store
	ns    string // namespace prefix

	mu      sync.Mutex
	nextSeq uint64
}

// NewManager creates a new WAL manager for the given namespace.
// It scans the store for existing WAL objects to determine the
// starting sequence number for recovery.
func NewManager(store objstore.Store, namespace string) (*Manager, error) {
	m := &Manager{store: store, ns: namespace}
	entries, err := m.listWALs(context.Background())
	if err != nil {
		return nil, fmt.Errorf("walcloud: listing WALs: %w", err)
	}
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		m.nextSeq = last.Seq + 1
	} else {
		m.nextSeq = 1
	}
	return m, nil
}

// walPath returns the full object store path for a WAL with the given seq.
func (m *Manager) walPath(seq uint64) string {
	return path.Join(m.ns, walDir, fmt.Sprintf("%0*d%s", seqNumWidth, seq, walExt))
}

// walPrefix returns the listing prefix for all WAL objects.
func (m *Manager) walPrefix() string {
	return path.Join(m.ns, walDir) + "/"
}

// WriteRecord creates a WAL object in object storage containing the given
// batch data. It blocks until the write is acknowledged. Returns the sequence
// number assigned to this WAL entry.
func (m *Manager) WriteRecord(ctx context.Context, data []byte) (seq uint64, err error) {
	m.mu.Lock()
	seq = m.nextSeq
	m.nextSeq++
	m.mu.Unlock()

	path := m.walPath(seq)
	if err := m.store.Put(ctx, path, data); err != nil {
		return 0, fmt.Errorf("walcloud: writing seq %d: %w", seq, err)
	}
	return seq, nil
}

type WalEntry struct {
	Seq  uint64
	Path string
}

func parseWALSeq(name string) (uint64, error) {
	name = strings.TrimSuffix(path.Base(name), walExt)
	return strconv.ParseUint(name, 10, 64)
}

func (m *Manager) listWALs(ctx context.Context) ([]WalEntry, error) {
	paths, err := m.store.List(ctx, m.walPrefix())
	if err != nil {
		return nil, err
	}
	var entries []WalEntry
	for _, p := range paths {
		seq, err := parseWALSeq(p)
		if err != nil {
			continue // skip malformed names
		}
		entries = append(entries, WalEntry{Seq: seq, Path: p})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq })
	return entries, nil
}

// List returns all WAL entries present in object storage, sorted by sequence
// number ascending.
func (m *Manager) List(ctx context.Context) ([]WalEntry, error) {
	return m.listWALs(ctx)
}

// ReadRecord downloads and returns the WAL entry for the given sequence number.
func (m *Manager) ReadRecord(ctx context.Context, seq uint64) ([]byte, error) {
	path := m.walPath(seq)
	data, err := m.store.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("walcloud: reading seq %d: %w", seq, err)
	}
	return data, nil
}

// GC deletes all WAL objects with sequence numbers <= maxSeq. These WALs are
// no longer needed because the data they contain has been flushed to SSTs and
// uploaded to object storage.
func (m *Manager) GC(ctx context.Context, maxSeq uint64) (deleted int, err error) {
	entries, err := m.listWALs(ctx)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.Seq > maxSeq {
			break // entries are sorted
		}
		if err := m.store.Delete(ctx, e.Path); err != nil {
			return deleted, fmt.Errorf("walcloud: gc deleting seq %d: %w", e.Seq, err)
		}
		deleted++
	}
	return deleted, nil
}

// NextSeq returns the next sequence number that will be assigned.
func (m *Manager) NextSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextSeq
}
