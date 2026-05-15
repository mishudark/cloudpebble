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
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ctx   context.Context

	batchWindow time.Duration // 0 = no batching

	mu      sync.Mutex
	nextSeq uint64

	// Batching state. Protected by mu.
	pending     [][]byte     // batch repr segments for current window
	pendingSeq  uint64
	waiters     []chan error // callers waiting for this batch to commit
	commitTimer *time.Timer
	closed      atomic.Bool
	wg          sync.WaitGroup // tracks in-flight batch flush goroutines
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
		atomic.StoreUint64(&m.nextSeq, last.Seq+1)
	} else {
		atomic.StoreUint64(&m.nextSeq, 1)
	}
	return m, nil
}

// SetContext sets the context used for background operations such as
// batch flush goroutines.
func (m *Manager) SetContext(ctx context.Context) {
	m.ctx = ctx
}

// Close stops the timer, prevents further writes, and waits for any
// in-flight batch flush to complete so that callers can safely close
// the underlying store afterwards.
func (m *Manager) Close() {
	m.closed.Store(true)
	m.mu.Lock()
	if m.commitTimer != nil {
		m.commitTimer.Stop()
		m.commitTimer = nil
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// Flush force-flushes the current batch window (if any), blocks until the
// GCS write completes or ctx expires, and then waits for any previously
// launched flush goroutines to finish. After Flush returns, all WAL data
// written before the call is durable in object storage.
func (m *Manager) Flush(ctx context.Context) error {
	m.mu.Lock()
	if m.commitTimer != nil {
		m.commitTimer.Stop()
		m.commitTimer = nil
	}
	segments := m.pending
	seq := m.pendingSeq
	waiters := m.waiters
	m.pending = nil
	m.pendingSeq = 0
	m.waiters = nil
	m.mu.Unlock()

	if len(segments) == 0 {
		m.wg.Wait()
		return nil
	}

	data := mergeBatchSegments(segments)
	p := m.walPath(seq)

	m.wg.Add(1)
	err := m.store.Put(ctx, p, data)
	m.wg.Done()

	for _, ch := range waiters {
		ch <- err
	}

	m.wg.Wait()
	return err
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
	if m.closed.Load() {
		return 0, nil, errors.New("walcloud: manager is closed")
	}
	if m.batchWindow <= 0 {
		// Direct write path: no mutex at all.
		// Sequence numbers are allocated atomically so writes are fully
		// concurrent — the only serialization is the underlying store.Put.
		seq = atomic.AddUint64(&m.nextSeq, 1) - 1
		p := m.walPath(seq)
		if err := m.store.Put(ctx, p, data); err != nil {
			return 0, nil, fmt.Errorf("walcloud: writing seq %d: %w", seq, err)
		}
		return seq, nil, nil
	}

	// Batching path: O(1) per writer under a short-lived mutex.
	m.mu.Lock()
	if m.pending == nil {
		m.pendingSeq = atomic.AddUint64(&m.nextSeq, 1) - 1
		m.pending = make([][]byte, 0, 16)
		m.commitTimer = time.AfterFunc(m.batchWindow, m.flushPending)
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	m.pending = append(m.pending, buf)
	ch := make(chan error, 1)
	m.waiters = append(m.waiters, ch)
	seq = m.pendingSeq
	m.mu.Unlock()

	return seq, ch, nil
}

// flushPending merges all pending segments into a single valid batch repr,
// writes it to object storage, and wakes all waiters. Called by the timer.
// The object storage Put runs in a goroutine so the next batch window can
// start accumulating immediately rather than waiting for the GCS roundtrip.
func (m *Manager) flushPending() {
	m.mu.Lock()
	segments := m.pending
	seq := m.pendingSeq
	waiters := m.waiters
	m.pending = nil
	m.pendingSeq = 0
	m.waiters = nil
	m.mu.Unlock()

	if len(segments) == 0 {
		return
	}

	data := mergeBatchSegments(segments)
	p := m.walPath(seq)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		var gcsErr error
		ctx := m.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := m.store.Put(ctx, p, data); err != nil {
			gcsErr = fmt.Errorf("walcloud: writing seq %d: %w", seq, err)
		}
		for _, ch := range waiters {
			ch <- gcsErr
		}
	}()
}

// batchHeaderLen is the Pebble batch repr header size (seqnum + count).
const batchHeaderLen = 12

// batchCount reads the record count from a Pebble batch repr header.
func batchCount(data []byte) uint32 {
	if len(data) < batchHeaderLen {
		return 0
	}
	return uint32(data[8]) | uint32(data[9])<<8 | uint32(data[10])<<16 | uint32(data[11])<<24
}

// setBatchCount writes the record count into a Pebble batch repr header.
func setBatchCount(data []byte, n uint32) {
	binary.LittleEndian.PutUint32(data[8:12], n)
}

// mergeBatchSegments merges N Pebble batch repr segments into one valid
// batch repr. The first segment's header is kept; subsequent headers are
// stripped. Falls back to raw concatenation for undersized segments.
func mergeBatchSegments(segments [][]byte) []byte {
	if len(segments) == 0 {
		return nil
	}
	if len(segments) == 1 {
		return segments[0]
	}

	// Compute total size with header stripping.
	total := len(segments[0])
	allValid := len(segments[0]) >= batchHeaderLen
	for _, s := range segments[1:] {
		if len(s) < 8 {
			allValid = false
			total += len(s) // can't strip header, just append raw
		} else if len(s) < batchHeaderLen {
			total += len(s)
		} else {
			total += len(s) - batchHeaderLen
		}
	}
	if !allValid {
		// Fall back to raw concatenation for undersized segments.
		result := make([]byte, 0, total)
		for _, s := range segments {
			result = append(result, s...)
		}
		return result
	}

	result := make([]byte, total)
	pos := copy(result, segments[0])
	totalCount := batchCount(segments[0])

	for _, s := range segments[1:] {
		totalCount += batchCount(s)
		if len(s) <= batchHeaderLen {
			continue
		}
		pos += copy(result[pos:], s[batchHeaderLen:])
	}

	setBatchCount(result, totalCount)
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
	return atomic.LoadUint64(&m.nextSeq)
}
