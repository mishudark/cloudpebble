// Package walcloud manages a write-ahead log stored in object storage.
// Each write produces an immutable WAL object. The local Pebble instance
// replays these WAL objects into its memtable for fast reads.
//
// WAL object paths follow the convention:
//
//	{namespace}/wal/{seqnum}.wal
//
// where seqnum is a zero-padded 20-digit monotonic counter.
//
// # Batching
//
// When BatchWindow is non-zero, concurrent writes within the window are
// coalesced into a single GCS WAL object. This amortizes the ~100ms GCS
// round-trip across multiple writes. Callers receive a sequence number
// immediately and wait on a channel for the GCS commit.
//
// With BatchWindow=0, each write creates its own WAL object synchronously
// (one GCS round-trip per write).
package walcloud

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	ns    string

	batchWindow time.Duration // 0 = no batching

	mu      sync.Mutex
	nextSeq uint64

	// Batching state. Protected by mu.
	pending    []byte        // accumulated batch data for current window
	pendingSeq uint64        // sequence number for the pending batch
	waiters    []chan error  // callers waiting for this batch to commit
	commitTimer *time.Timer
}

// NewManager creates a new WAL manager for the given namespace.
// It scans the store for existing WAL objects to determine the
// starting sequence number for recovery.
//
// batchWindow is the maximum time to hold a batch open before committing
// to object storage. Zero means no batching (each write gets its own WAL
// object). A typical value is 1 second.
func NewManager(store objstore.Store, namespace string, batchWindow time.Duration) (*Manager, error) {
	m := &Manager{
		store:       store,
		ns:          namespace,
		batchWindow: batchWindow,
	}
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

// WriteRecord writes the given batch data to a WAL object in object storage.
//
// When batching is disabled (BatchWindow=0), this blocks until the GCS write
// completes and the returned channel is nil.
//
// When batching is enabled, the data is appended to an in-memory batch. The
// sequence number is returned immediately. The caller MUST wait on the
// returned channel to know when the batch has been durably committed to GCS.
// If the channel receives an error, the batch was NOT committed.
//
// Sequence numbers are assigned monotonically and returned immediately.
func (m *Manager) WriteRecord(ctx context.Context, data []byte) (seq uint64, done <-chan error, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.batchWindow <= 0 {
		// Direct write path: no batching.
		return m.writeDirectLocked(ctx, data)
	}

	// Batching path.
	if m.pending == nil {
		// First write in the window: allocate a seq and start the timer.
		m.pendingSeq = m.nextSeq
		m.nextSeq++
		m.pending = make([]byte, len(data))
		copy(m.pending, data)
		ch := make(chan error, 1)
		m.waiters = []chan error{ch}
		m.commitTimer = time.AfterFunc(m.batchWindow, m.flushPending)
		return m.pendingSeq, ch, nil
	}

	// Subsequent write in the window: merge into the pending batch.
	m.pending = appendBatch(m.pending, data)
	ch := make(chan error, 1)
	m.waiters = append(m.waiters, ch)
	return m.pendingSeq, ch, nil
}

// writeDirectLocked creates a single WAL object synchronously. Must be
// called with m.mu held.
func (m *Manager) writeDirectLocked(ctx context.Context, data []byte) (uint64, <-chan error, error) {
	seq := m.nextSeq
	m.nextSeq++

	p := m.walPath(seq)
	if err := m.store.Put(ctx, p, data); err != nil {
		return 0, nil, fmt.Errorf("walcloud: writing seq %d: %w", seq, err)
	}
	return seq, nil, nil
}

// flushPending commits the current pending batch to object storage and
// wakes all waiters. Called by the commit timer.
func (m *Manager) flushPending() {
	m.mu.Lock()
	data := m.pending
	seq := m.pendingSeq
	waiters := m.waiters
	m.pending = nil
	m.pendingSeq = 0
	m.waiters = nil
	m.mu.Unlock()

	if data == nil {
		return
	}

	var gcsErr error
	p := m.walPath(seq)
	if err := m.store.Put(context.Background(), p, data); err != nil {
		gcsErr = fmt.Errorf("walcloud: writing seq %d: %w", seq, err)
	}

	for _, ch := range waiters {
		ch <- gcsErr
	}
}

// appendBatch merges two Pebble batch representations into one. Both are
// self-contained batch reprs. We strip the header from the second batch
// and append only its records to the first, then update the count header.
func appendBatch(into, extra []byte) []byte {
	if len(into) < 8 || len(extra) < 8 {
		return append(into, extra...)
	}
	// Pebble batch repr layout: [8-byte header: seqnum(8) count(4)...] records...
	// We need to append the extra batch's records to into and update the count.
	// The header is 12 bytes (seqnum + count), followed by records.
	const headerLen = 12
	if len(into) < headerLen || len(extra) < headerLen {
		return append(into, extra...)
	}
	// Read current count from into
	curCount := int(into[8]) | int(into[9])<<8 | int(into[10])<<16 | int(into[11])<<24
	// Read extra count from extra
	extraCount := int(extra[8]) | int(extra[9])<<8 | int(extra[10])<<16 | int(extra[11])<<24

	// Append everything after the header from extra
	result := append(into, extra[headerLen:]...)

	// Update count
	newCount := curCount + extraCount
	result[8] = byte(newCount)
	result[9] = byte(newCount >> 8)
	result[10] = byte(newCount >> 16)
	result[11] = byte(newCount >> 24)

	return result
}

// ---------------------------------------------------------------------------
// WAL entry listing / reading / GC
// ---------------------------------------------------------------------------

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
			continue
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
	p := m.walPath(seq)
	data, err := m.store.Get(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("walcloud: reading seq %d: %w", seq, err)
	}
	return data, nil
}

// GC deletes WAL objects that are no longer needed.
func (m *Manager) GC(ctx context.Context, maxSeq uint64, orphanTTL time.Duration) (deleted int, err error) {
	entries, err := m.listWALs(ctx)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.Seq <= maxSeq {
			if err := m.store.Delete(ctx, e.Path); err != nil {
				return deleted, fmt.Errorf("walcloud: gc deleting seq %d: %w", e.Seq, err)
			}
			deleted++
			continue
		}
		if orphanTTL <= 0 {
			continue
		}
		info, err := m.store.Attrs(ctx, e.Path)
		if err != nil {
			continue
		}
		if time.Since(info.CreatedAt) > orphanTTL {
			if err := m.store.Delete(ctx, e.Path); err != nil {
				return deleted, fmt.Errorf("walcloud: gc deleting orphan seq %d: %w", e.Seq, err)
			}
			deleted++
		}
	}
	return deleted, nil
}

// NextSeq returns the next sequence number that will be assigned.
func (m *Manager) NextSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nextSeq
}
