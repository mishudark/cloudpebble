// Package engine implements a key-value storage engine that provides durable
// writes via an object-storage-backed write-ahead log (WAL) and local Pebble
// LSM for fast reads.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/mishudark/cloudpebble/pkg/objstore"
	"github.com/mishudark/cloudpebble/pkg/walcloud"
)

const (
	defaultSyncInterval      = 30 * time.Second
	defaultColdMissThreshold = 3
	manifestName             = "manifest"
	dataDir                  = "data"
	ckptDir                  = "checkpoint"
	manifestsDir             = "manifests"
	maxManifestHistory       = 10
)

// ConsistencyLevel controls the consistency guarantee on reads.
type ConsistencyLevel int

const (
	ConsistencyStrong ConsistencyLevel = iota
	ConsistencyEventual
)

// Manifest is the metadata file stored in object storage that records the
// last checkpoint version, its file inventory with checksums, and the
// highest WAL sequence number covered by that checkpoint.
type Manifest struct {
	Version     int            `json:"version"`
	MaxWALSeq   uint64         `json:"max_wal_seq"`
	CreatedAt   time.Time      `json:"created_at"`
	PrevVersion int            `json:"prev_version"`
	Files       []ManifestFile `json:"files"`
}

type ManifestFile struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"` // SHA-256 hex
}

// Options configures an Engine.
type Options struct {
	// Dir is the local directory used for the Pebble instance cache.
	Dir string

	// Store is the object storage backend for durable data.
	Store objstore.Store

	// Namespace is the tenant/namespace identifier.
	Namespace string

	// SyncInterval controls how often local SSTs are uploaded to object
	// storage. If zero, defaults to 30 seconds.
	SyncInterval time.Duration

	// ColdMissThreshold is the number of consecutive cache misses before
	// the engine triggers a recovery download from object storage.
	// Zero disables automatic cold-miss recovery. Default: 3.
	ColdMissThreshold int

	// Consistency sets the read consistency level. Default: ConsistencyStrong.
	Consistency ConsistencyLevel

	// OrphanWALTTL is the duration after which WAL objects with sequence
	// numbers that have not been applied locally are deleted as orphans.
	// Zero disables orphan cleanup. Default: 1 hour.
	OrphanWALTTL time.Duration

	// BatchWindow controls WAL batching. Concurrent writes within this
	// window are coalesced into a single GCS WAL object, amortizing GCS
	// round-trips across many writers. Set to a negative value to disable
	// batching (each write creates its own WAL object). Zero means use
	// the default (200ms).
	BatchWindow time.Duration

	// MaxLocalBytes is the soft limit on the local Pebble cache size. When
	// exceeded, the engine compacts older data to reduce disk usage. Zero
	// means no limit. Default: 0.
	MaxLocalBytes int64

	// Logger is the structured logger used for engine events. If nil, a
	// default logger writing to os.Stderr is used.
	Logger *slog.Logger

	// PebbleOptions are passed through to the underlying Pebble instance.
	// The Engine forces DisableWAL to true regardless of this setting.
	PebbleOptions *pebble.Options
}

// Engine is the core CloudPebble instance for a single namespace.
type Engine struct {
	ns       string
	store    objstore.Store
	walMgr   *walcloud.Manager
	localDir string
	logger   *slog.Logger

	consistency ConsistencyLevel

	dbMu sync.RWMutex
	db   *pebble.DB

	mu        sync.Mutex
	maxWALSeq uint64

	syncMu        sync.Mutex
	uploadedMu    sync.Mutex
	uploadedFiles map[string]struct{}

	orphanWALTTL time.Duration
	batchWindow  time.Duration

	maxLocalBytes   int64
	manifestVersion int

	pebbleOpts *pebble.Options

	metrics Metrics

	coldMissCount  atomic.Int64
	recovering     atomic.Bool
	coldMissThresh int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	errCh   chan error
	healthy atomic.Bool
	ready   atomic.Bool
}

// Open creates or recovers an Engine for the given namespace.
func Open(ctx context.Context, opts Options) (*Engine, error) {
	if opts.Dir == "" {
		opts.Dir = os.TempDir()
	}
	if opts.Store == nil {
		return nil, errors.New("engine: objstore.Store is required")
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaultSyncInterval
	}
	if opts.ColdMissThreshold <= 0 {
		opts.ColdMissThreshold = defaultColdMissThreshold
	}
	if opts.OrphanWALTTL <= 0 {
		opts.OrphanWALTTL = 1 * time.Hour
	}
	if opts.BatchWindow == 0 {
		opts.BatchWindow = 200 * time.Millisecond
	}
	if opts.PebbleOptions == nil {
		opts.PebbleOptions = &pebble.Options{}
	}
	opts.PebbleOptions.EnsureDefaults()
	opts.PebbleOptions.DisableWAL = true

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	walMgr, err := walcloud.NewManager(opts.Store, opts.Namespace, opts.BatchWindow)
	if err != nil {
		return nil, fmt.Errorf("engine: creating WAL manager: %w", err)
	}

	if err := os.MkdirAll(opts.Dir, 0750); err != nil {
		return nil, fmt.Errorf("engine: creating local dir: %w", err)
	}

	e := &Engine{
		ns:             opts.Namespace,
		store:          opts.Store,
		walMgr:         walMgr,
		localDir:       opts.Dir,
		logger:         opts.Logger,
		consistency:    opts.Consistency,
		orphanWALTTL:   opts.OrphanWALTTL,
		batchWindow:    opts.BatchWindow,
		maxLocalBytes:  opts.MaxLocalBytes,
		pebbleOpts:     opts.PebbleOptions,
		uploadedFiles:  make(map[string]struct{}),
		coldMissThresh: int64(opts.ColdMissThreshold),
		errCh:          make(chan error, 16),
	}

	if err := e.recover(ctx); err != nil {
		return nil, fmt.Errorf("engine: recovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.ctx = ctx
	e.cancel = cancel

	e.walMgr.SetContext(ctx)

	e.wg.Add(1)
	go e.syncLoop(opts.SyncInterval)

	if e.consistency == ConsistencyEventual {
		e.wg.Add(1)
		go e.walReplayLoop()
	}

	e.healthy.Store(true)
	e.ready.Store(true)

	return e, nil
}

// recover downloads checkpoint data from object storage (if any) and
// replays uncommitted WAL entries. It opens Pebble with the recovered
// state. Called both at Open() and during cold-miss recovery.
func (e *Engine) recover(ctx context.Context) error {
	manifestBytes, err := e.store.Get(ctx, e.manifestPath())
	hasManifest := err == nil

	e.uploadedMu.Lock()
	e.uploadedFiles = make(map[string]struct{})
	e.uploadedMu.Unlock()

	if hasManifest {
		var m Manifest
		if err = json.Unmarshal(manifestBytes, &m); err != nil {
			var old struct{ MaxWALSeq uint64 }
			if err2 := json.Unmarshal(manifestBytes, &old); err2 != nil {
				return fmt.Errorf("decoding manifest: %w", err)
			}
			m.MaxWALSeq = old.MaxWALSeq
		}

		e.manifestVersion = m.Version

		for _, mf := range m.Files {
			remotePath := filepath.ToSlash(filepath.Join(e.dataPrefix(), mf.Name))
			localPath := filepath.Join(e.localDir, mf.Name)
			var data []byte
			data, err = e.store.Get(ctx, remotePath)
			if err != nil {
				return fmt.Errorf("downloading %s: %w", remotePath, err)
			}
			err = os.WriteFile(localPath, data, 0600)
			if err != nil {
				return fmt.Errorf("writing local %s: %w", localPath, err)
			}
			e.uploadedMu.Lock()
			e.uploadedFiles[remotePath] = struct{}{}
			e.uploadedMu.Unlock()
		}

		e.maxWALSeq = m.MaxWALSeq
	}

	db, err := pebble.Open(e.localDir, e.pebbleOpts)
	if err != nil {
		return fmt.Errorf("opening pebble: %w", err)
	}

	e.dbMu.Lock()
	if e.db != nil {
		_ = e.db.Close()
	}
	e.db = db
	e.dbMu.Unlock()

	if e.consistency == ConsistencyStrong {
		if err := e.replayWALs(ctx); err != nil {
			return fmt.Errorf("replaying WALs: %w", err)
		}
	}

	return nil
}

func (e *Engine) replayWALs(ctx context.Context) error {
	entries, err := e.walMgr.List(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Seq <= e.maxWALSeq {
			continue
		}
		data, err := e.walMgr.ReadRecord(ctx, entry.Seq)
		if err != nil {
			return fmt.Errorf("reading WAL seq %d: %w", entry.Seq, err)
		}
		batch := e.db.NewBatch()
		if err := batch.SetRepr(data); err != nil {
			_ = batch.Close()
			return fmt.Errorf("setting WAL repr seq %d: %w", entry.Seq, err)
		}
		if err := e.db.Apply(batch, pebble.NoSync); err != nil {
			_ = batch.Close()
			return fmt.Errorf("replaying WAL seq %d: %w", entry.Seq, err)
		}
		_ = batch.Close()

		e.mu.Lock()
		if entry.Seq > e.maxWALSeq {
			e.maxWALSeq = entry.Seq
		}
		e.mu.Unlock()
	}
	return nil
}

// walReplayLoop periodically replays WAL entries that were skipped during
// eventual-consistency open. This self-heals the node by gradually catching
// it up to the latest durable state.
func (e *Engine) walReplayLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.replayWALs(e.ctx); err != nil {
				e.logger.Error("WAL replay error", "error", err)
				select {
				case e.errCh <- fmt.Errorf("wal replay: %w", err):
				default:
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

func (e *Engine) writeWALAndApply(ctx context.Context, batch *pebble.Batch) (uint64, error) {
	data := batch.Repr()

	walStart := time.Now()
	seq, done, err := e.walMgr.WriteRecord(ctx, data)
	e.metrics.WALWriteLatencyNs.Add(time.Since(walStart).Nanoseconds())
	if err != nil {
		return 0, fmt.Errorf("engine: writing WAL: %w", err)
	}
	e.metrics.WALObjectsWritten.Add(1)
	e.metrics.BytesWrittenWAL.Add(int64(len(data)))

	if done != nil {
		select {
		case gcsErr := <-done:
			if gcsErr != nil {
				return 0, fmt.Errorf("engine: WAL durability: %w", gcsErr)
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	applyStart := time.Now()
	e.dbMu.RLock()
	err = e.db.Apply(batch, pebble.NoSync)
	e.dbMu.RUnlock()
	e.metrics.ApplyLatencyNs.Add(time.Since(applyStart).Nanoseconds())
	if err != nil {
		e.logger.Warn("local apply failed after durable WAL commit", "seq", seq, "error", err)
		return 0, fmt.Errorf("engine: applying: %w", err)
	}

	e.mu.Lock()
	if seq > e.maxWALSeq {
		e.maxWALSeq = seq
	}
	e.mu.Unlock()
	return seq, nil
}

// Set stores a key-value pair. The write is durably committed to object
// storage before returning.
func (e *Engine) Set(ctx context.Context, key, value []byte) error {
	batch := e.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Set(key, value, nil); err != nil {
		return fmt.Errorf("engine: building set: %w", err)
	}

	_, err := e.writeWALAndApply(ctx, batch)
	if err == nil {
		e.metrics.Sets.Add(1)
	}
	return err
}

// Delete removes a key. The deletion is durably committed to object storage
// before returning.
func (e *Engine) Delete(ctx context.Context, key []byte) error {
	batch := e.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Delete(key, nil); err != nil {
		return fmt.Errorf("engine: building delete: %w", err)
	}

	_, err := e.writeWALAndApply(ctx, batch)
	if err == nil {
		e.metrics.Deletes.Add(1)
	}
	return err
}

// Apply writes a pre-built batch durably. The batch is committed to object
// storage WAL before being applied locally.
func (e *Engine) Apply(ctx context.Context, batch *pebble.Batch) error {
	_, err := e.writeWALAndApply(ctx, batch)
	return err
}

// ---------------------------------------------------------------------------
// Read path with cold-miss fallback
// ---------------------------------------------------------------------------

// Get retrieves the value for a key. If the key is not found in the local
// cache and the cold-miss threshold is exceeded, the engine triggers a
// background recovery from object storage.
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.dbMu.RLock()
	db := e.db
	e.dbMu.RUnlock()

	if e.recovering.Load() {
		e.dbMu.RLock()
		db = e.db
		e.dbMu.RUnlock()
	}

	val, closer, err := db.Get(key)
	if err != nil {
		e.metrics.Gets.Add(1)
		if err == pebble.ErrNotFound {
			e.metrics.GetMisses.Add(1)
			if e.coldMissThresh > 0 && !e.recovering.Load() {
				n := e.coldMissCount.Add(1)
				if n >= e.coldMissThresh {
					e.triggerColdRecovery()
				}
			}
		}
		return nil, err
	}
	e.metrics.Gets.Add(1)
	e.metrics.GetHits.Add(1)
	e.coldMissCount.Store(0)
	result := make([]byte, len(val))
	copy(result, val)
	_ = closer.Close()
	return result, nil
}

func (e *Engine) triggerColdRecovery() {
	if !e.recovering.CompareAndSwap(false, true) {
		return
	}
	e.wg.Go(func() {
		defer e.recovering.Store(false)
		defer e.coldMissCount.Store(0)

		e.metrics.ColdRecoveries.Add(1)

		ctx, cancel := context.WithTimeout(e.ctx, 5*time.Minute)
		defer cancel()

		if err := e.recover(ctx); err != nil {
			e.logger.Error("cold recovery failed", "error", err)
			select {
			case e.errCh <- fmt.Errorf("cold recovery: %w", err):
			default:
			}
		}
	})
}

// ---------------------------------------------------------------------------
// DB access
// ---------------------------------------------------------------------------

// DB returns the underlying local Pebble instance. Callers must NOT close
// the returned DB. The Engine may swap the DB during cold-miss recovery.
func (e *Engine) DB() *pebble.DB {
	e.dbMu.RLock()
	defer e.dbMu.RUnlock()
	return e.db
}

// Metrics returns the engine's operational metrics. All counters are safe
// for concurrent access.
func (e *Engine) Metrics() *Metrics {
	return &e.metrics
}

// ---------------------------------------------------------------------------
// Health and readiness
// ---------------------------------------------------------------------------

// Health returns nil if the engine is healthy, or an error describing the
// most recent background failure.
func (e *Engine) Health() error {
	if !e.healthy.Load() {
		return errors.New("engine: unhealthy")
	}
	return nil
}

// Ready reports whether the engine has completed recovery and is serving requests.
func (e *Engine) Ready() bool {
	return e.ready.Load()
}

// Errors returns a read-only channel that receives errors from background
// goroutines (sync loop, WAL replay, cold recovery). The channel is buffered;
// if the buffer is full, errors are dropped. Callers should drain the channel
// periodically.
func (e *Engine) Errors() <-chan error {
	return e.errCh
}

// ---------------------------------------------------------------------------
// Sync (flush + upload checkpoint)
// ---------------------------------------------------------------------------

// Sync flushes the local Pebble instance and uploads a consistent checkpoint
// to object storage. After Sync returns, all data up to the current point is
// durably stored in object storage as both WAL entries and SST files.
// Only new or changed SST files are uploaded (incremental).
func (e *Engine) Sync(ctx context.Context) (err error) {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()

	start := time.Now()
	defer func() {
		e.metrics.SyncLatencyNs.Add(time.Since(start).Nanoseconds())
		e.metrics.SyncCalls.Add(1)
		if err != nil {
			e.metrics.SyncFailures.Add(1)
		}
	}()

	e.dbMu.RLock()
	db := e.db
	e.dbMu.RUnlock()

	flushDone, err := db.AsyncFlush()
	if err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}
	<-flushDone

	if err = e.walMgr.Flush(ctx); err != nil {
		return fmt.Errorf("engine: flushing WAL batch: %w", err)
	}

	checkpointDir := filepath.Join(e.localDir, ckptDir)
	_ = os.RemoveAll(checkpointDir)
	defer func() { _ = os.RemoveAll(checkpointDir) }()

	if err = db.Checkpoint(checkpointDir); err != nil {
		return fmt.Errorf("engine: checkpoint: %w", err)
	}

	entries, err := os.ReadDir(checkpointDir)
	if err != nil {
		return fmt.Errorf("engine: reading checkpoint dir: %w", err)
	}

	e.mu.Lock()
	currentSeq := e.maxWALSeq
	dataPrefix := e.dataPrefix()
	e.mu.Unlock()

	isMutable := func(name string) bool {
		return name == "CURRENT" ||
			strings.HasPrefix(name, "MANIFEST-") ||
			strings.HasPrefix(name, "OPTIONS-") ||
			strings.HasPrefix(name, "marker.format-version") ||
			strings.HasPrefix(name, "marker.manifest")
	}

	checkpointFiles := make(map[string]bool, len(entries))
	manifestFiles := make([]ManifestFile, 0, len(entries))
	for _, entry := range entries {
		checkpointFiles[entry.Name()] = true

		localPath := filepath.Clean(filepath.Join(checkpointDir, entry.Name()))
		var data []byte
		data, err = os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("engine: reading checkpoint file %s: %w", entry.Name(), err)
		}

		h := sha256.Sum256(data)
		manifestFiles = append(manifestFiles, ManifestFile{
			Name:     entry.Name(),
			Size:     int64(len(data)),
			Checksum: hex.EncodeToString(h[:]),
		})

		remotePath := filepath.ToSlash(filepath.Join(dataPrefix, entry.Name()))

		e.uploadedMu.Lock()
		_, alreadyUploaded := e.uploadedFiles[remotePath]
		e.uploadedMu.Unlock()

		if alreadyUploaded && !isMutable(entry.Name()) {
			continue
		}

		if err = e.store.Put(ctx, remotePath, data); err != nil {
			return fmt.Errorf("engine: uploading %s: %w", entry.Name(), err)
		}

		e.uploadedMu.Lock()
		e.uploadedFiles[remotePath] = struct{}{}
		e.uploadedMu.Unlock()
	}

	e.uploadedMu.Lock()
	for remotePath := range e.uploadedFiles {
		base := filepath.Base(remotePath)
		if !checkpointFiles[base] {
			if err = e.store.Delete(ctx, remotePath); err != nil {
				e.uploadedMu.Unlock()
				return fmt.Errorf("engine: deleting stale file %s: %w", base, err)
			}
			delete(e.uploadedFiles, remotePath)
		}
	}
	e.uploadedMu.Unlock()

	mf := Manifest{
		Version:     e.manifestVersion + 1,
		PrevVersion: e.manifestVersion,
		MaxWALSeq:   currentSeq,
		CreatedAt:   time.Now(),
		Files:       manifestFiles,
	}
	manifestBytes, err := json.Marshal(mf)
	if err != nil {
		return fmt.Errorf("engine: encoding manifest: %w", err)
	}

	// Write versioned manifest first, then update the current manifest pointer.
	// If the versioned write succeeds but the current pointer write fails,
	// the versioned copy can be used for recovery.
	if err = e.store.Put(ctx, e.manifestVersionPath(mf.Version), manifestBytes); err != nil {
		return fmt.Errorf("engine: writing versioned manifest: %w", err)
	}

	if err = e.store.Put(ctx, e.manifestPath(), manifestBytes); err != nil {
		return fmt.Errorf("engine: writing manifest: %w", err)
	}

	oldVersions, err := e.store.List(ctx, e.manifestVersionsPrefix())
	if err == nil && len(oldVersions) > maxManifestHistory {
		sort.Strings(oldVersions)
		for _, v := range oldVersions[:len(oldVersions)-maxManifestHistory] {
			_ = e.store.Delete(ctx, v)
		}
	}

	e.manifestVersion = mf.Version

	deleted, err := e.walMgr.GC(ctx, currentSeq, e.orphanWALTTL)
	if err != nil {
		e.metrics.SyncFailures.Add(1)
		return fmt.Errorf("engine: WAL GC: %w", err)
	}
	e.metrics.WALObjectsGCd.Add(int64(deleted))

	return nil
}

func (e *Engine) syncLoop(interval time.Duration) {
	defer e.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.Sync(e.ctx); err != nil {
				e.logger.Error("sync error", "error", err)
				e.healthy.Store(false)
				select {
				case e.errCh <- fmt.Errorf("sync: %w", err):
				default:
				}
			} else {
				e.healthy.Store(true)
			}
			e.checkEviction()
		}
	}
}

func (e *Engine) checkEviction() {
	if e.maxLocalBytes <= 0 {
		return
	}

	e.dbMu.RLock()
	db := e.db
	e.dbMu.RUnlock()

	m := db.Metrics()
	if m.DiskSpaceUsage() <= uint64(e.maxLocalBytes) {
		return
	}

	_ = db.Compact(e.ctx, nil, nil, true)
	_ = e.Sync(e.ctx)
}

func (e *Engine) manifestPath() string {
	return filepath.ToSlash(filepath.Join(e.ns, manifestName))
}

func (e *Engine) manifestVersionPath(v int) string {
	return filepath.ToSlash(filepath.Join(e.ns, manifestsDir, fmt.Sprintf("%06d.json", v)))
}

func (e *Engine) manifestVersionsPrefix() string {
	return filepath.ToSlash(filepath.Join(e.ns, manifestsDir)) + "/"
}

func (e *Engine) dataPrefix() string {
	return filepath.ToSlash(filepath.Join(e.ns, dataDir)) + "/"
}

// Close gracefully shuts down the engine, flushing data and uploading a final
// checkpoint. It waits for all background goroutines to finish.
func (e *Engine) Close() error {
	e.cancel()
	e.wg.Wait()

	e.walMgr.Close()

	if err := e.Sync(context.Background()); err != nil {
		e.dbMu.RLock()
		if e.db != nil {
			_ = e.db.Close()
		}
		e.dbMu.RUnlock()
		return fmt.Errorf("engine: final sync: %w", err)
	}
	e.dbMu.RLock()
	defer e.dbMu.RUnlock()
	return e.db.Close()
}
