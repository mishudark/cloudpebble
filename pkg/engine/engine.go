package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	Version     int              `json:"version"`
	MaxWALSeq   uint64           `json:"max_wal_seq"`
	CreatedAt   time.Time        `json:"created_at"`
	PrevVersion int              `json:"prev_version"`
	Files       []ManifestFile   `json:"files"`
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
	// window are coalesced into a single GCS WAL object. Zero disables
	// batching. Default: 1s (matching turbopuffer's 1 WAL entry/sec).
	BatchWindow time.Duration

	// MaxLocalBytes is the soft limit on the local Pebble cache size. When
	// exceeded, the engine compacts older data to reduce disk usage. Zero
	// means no limit. Default: 0.
	MaxLocalBytes int64

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

	consistency ConsistencyLevel

	dbMu sync.RWMutex
	db   *pebble.DB

	mu        sync.Mutex
	maxWALSeq uint64

	uploadedMu    sync.Mutex
	uploadedFiles map[string]struct{}

	orphanWALTTL time.Duration
	batchWindow  time.Duration

	maxLocalBytes   int64
	manifestVersion int

	metrics Metrics

	coldMissCount  atomic.Int64
	recovering     atomic.Bool
	coldMissThresh int64

	ctx    context.Context
	cancel context.CancelFunc
}

// Open creates or recovers an Engine for the given namespace.
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		opts.Dir = os.TempDir()
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("engine: objstore.Store is required")
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
		opts.BatchWindow = 1 * time.Second
	}
	if opts.PebbleOptions == nil {
		opts.PebbleOptions = &pebble.Options{}
	}
	opts.PebbleOptions.EnsureDefaults()
	opts.PebbleOptions.DisableWAL = true

	walMgr, err := walcloud.NewManager(opts.Store, opts.Namespace, opts.BatchWindow)
	if err != nil {
		return nil, fmt.Errorf("engine: creating WAL manager: %w", err)
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine: creating local dir: %w", err)
	}

	e := &Engine{
		ns:             opts.Namespace,
		store:          opts.Store,
		walMgr:         walMgr,
		localDir:       opts.Dir,
		consistency:    opts.Consistency,
		orphanWALTTL:   opts.OrphanWALTTL,
		batchWindow:    opts.BatchWindow,
		maxLocalBytes:  opts.MaxLocalBytes,
		uploadedFiles:  make(map[string]struct{}),
		coldMissThresh: int64(opts.ColdMissThreshold),
	}

	if err := e.recover(context.Background()); err != nil {
		return nil, fmt.Errorf("engine: recovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.ctx = ctx
	e.cancel = cancel

	go e.syncLoop(opts.SyncInterval)

	if e.consistency == ConsistencyEventual {
		go e.walReplayLoop()
	}

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
		if err := json.Unmarshal(manifestBytes, &m); err != nil {
			// Try old format: {MaxWALSeq: N}
			var old struct{ MaxWALSeq uint64 }
			if err2 := json.Unmarshal(manifestBytes, &old); err2 != nil {
				return fmt.Errorf("decoding manifest: %w", err)
			}
			m.MaxWALSeq = old.MaxWALSeq
		}

		e.manifestVersion = m.Version

		dataPrefix := e.dataPrefix()
		files, err := e.store.List(ctx, dataPrefix)
		if err != nil {
			return fmt.Errorf("listing data files: %w", err)
		}
		for _, f := range files {
			localName := filepath.Base(f)
			localPath := filepath.Join(e.localDir, localName)
			data, err := e.store.Get(ctx, f)
			if err != nil {
				return fmt.Errorf("downloading %s: %w", f, err)
			}
			if err := os.WriteFile(localPath, data, 0644); err != nil {
				return fmt.Errorf("writing local %s: %w", localPath, err)
			}
			e.uploadedMu.Lock()
			e.uploadedFiles[f] = struct{}{}
			e.uploadedMu.Unlock()
		}

		e.maxWALSeq = m.MaxWALSeq
	}

	db, err := pebble.Open(e.localDir, &pebble.Options{
		DisableWAL: true,
	})
	if err != nil {
		return fmt.Errorf("opening pebble: %w", err)
	}

	// Close previous db if this is a mid-life recovery (cold miss trigger).
	e.dbMu.Lock()
	if e.db != nil {
		e.db.Close()
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
			batch.Close()
			return fmt.Errorf("setting WAL repr seq %d: %w", entry.Seq, err)
		}
		if err := e.db.Apply(batch, pebble.NoSync); err != nil {
			batch.Close()
			return fmt.Errorf("replaying WAL seq %d: %w", entry.Seq, err)
		}
		batch.Close()

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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.replayWALs(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "engine: WAL replay error: %v\n", err)
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

	// Apply to local Pebble immediately. Writes are visible before the
	// GCS commit, trading a small window of potential data loss (if the
	// process crashes before the GCS write completes) for low latency.
	applyStart := time.Now()
	e.dbMu.RLock()
	err = e.db.Apply(batch, pebble.NoSync)
	e.dbMu.RUnlock()
	e.metrics.ApplyLatencyNs.Add(time.Since(applyStart).Nanoseconds())
	if err != nil {
		return 0, fmt.Errorf("engine: applying: %w", err)
	}

	// Wait for GCS durability if batching is enabled.
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
	defer batch.Close()

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
	defer batch.Close()

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
	closer.Close()
	return result, nil
}

func (e *Engine) triggerColdRecovery() {
	if !e.recovering.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.recovering.Store(false)
		defer e.coldMissCount.Store(0)

		e.metrics.ColdRecoveries.Add(1)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := e.recover(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "engine: cold recovery failed: %v\n", err)
		}
	}()
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
// Sync (flush + upload checkpoint)
// ---------------------------------------------------------------------------

// Sync flushes the local Pebble instance and uploads a consistent checkpoint
// to object storage. After Sync returns, all data up to the current point is
// durably stored in object storage as both WAL entries and SST files.
// Only new or changed SST files are uploaded (incremental).
func (e *Engine) Sync(ctx context.Context) (err error) {
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

	// Use async flush so new writes can proceed into the next memtable
	// while the current one is flushed to SSTs. Only the checkpoint
	// step blocks writes (briefly, for the file copy).
	flushDone, err := db.AsyncFlush()
	if err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}
	<-flushDone

	checkpointDir := filepath.Join(e.localDir, ckptDir)
	os.RemoveAll(checkpointDir)
	defer os.RemoveAll(checkpointDir)

	if err := db.Checkpoint(checkpointDir); err != nil {
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

	// Files that can change content without changing name (MANIFEST,
	// OPTIONS, markers) must always be uploaded. SST files are immutable
	// (new file number on each compaction/flush) so they can be skipped
	// by name.
	mutableNames := map[string]bool{
		"MANIFEST":               true,
		"OPTIONS":                true,
		"marker.format-version":  true,
		"marker.manifest":        true,
		"CURRENT":                true,
	}

	isMutable := func(name string) bool {
		for prefix := range mutableNames {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}
		return false
	}

	checkpointFiles := make(map[string]bool, len(entries))
	for _, entry := range entries {
		checkpointFiles[entry.Name()] = true

		remotePath := filepath.ToSlash(filepath.Join(dataPrefix, entry.Name()))

		e.uploadedMu.Lock()
		_, alreadyUploaded := e.uploadedFiles[remotePath]
		e.uploadedMu.Unlock()

		if alreadyUploaded && !isMutable(entry.Name()) {
			continue
		}

		localPath := filepath.Join(checkpointDir, entry.Name())
		data, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("engine: reading checkpoint file %s: %w", entry.Name(), err)
		}
		if err := e.store.Put(ctx, remotePath, data); err != nil {
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
			if err := e.store.Delete(ctx, remotePath); err != nil {
				e.uploadedMu.Unlock()
				return fmt.Errorf("engine: deleting stale file %s: %w", base, err)
			}
			delete(e.uploadedFiles, remotePath)
		}
	}
	e.uploadedMu.Unlock()

	// Build manifest with checksums and file inventory.
	mf := Manifest{
		Version:     e.manifestVersion + 1,
		PrevVersion: e.manifestVersion,
		MaxWALSeq:   currentSeq,
		CreatedAt:   time.Now(),
	}
	for _, entry := range entries {
		localPath := filepath.Join(checkpointDir, entry.Name())
		data, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("engine: reading %s for checksum: %w", entry.Name(), err)
		}
		h := sha256.Sum256(data)
		mf.Files = append(mf.Files, ManifestFile{
			Name:     entry.Name(),
			Size:     int64(len(data)),
			Checksum: hex.EncodeToString(h[:]),
		})
	}
	manifestBytes, err := json.Marshal(mf)
	if err != nil {
		return fmt.Errorf("engine: encoding manifest: %w", err)
	}

	// Write the current manifest.
	if err := e.store.Put(ctx, e.manifestPath(), manifestBytes); err != nil {
		return fmt.Errorf("engine: writing manifest: %w", err)
	}

	// Write versioned manifest for rollback history.
	if err := e.store.Put(ctx, e.manifestVersionPath(mf.Version), manifestBytes); err != nil {
		return fmt.Errorf("engine: writing versioned manifest: %w", err)
	}

	// Prune old manifest versions.
	oldVersions, err := e.store.List(ctx, e.manifestVersionsPrefix())
	if err == nil && len(oldVersions) > maxManifestHistory {
		sort.Strings(oldVersions)
		for _, v := range oldVersions[:len(oldVersions)-maxManifestHistory] {
			e.store.Delete(ctx, v)
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.Sync(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "engine: sync error: %v\n", err)
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

	// Trigger a full compaction to reduce SST count and reclaim space.
	// Old SST files will be detected as stale by the next Sync and deleted
	// from GCS.
	db.Compact(context.Background(), nil, nil, true)
	e.Sync(context.Background())
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
// checkpoint.
func (e *Engine) Close() error {
	e.cancel()

	if err := e.Sync(context.Background()); err != nil {
		e.dbMu.RLock()
		if e.db != nil {
			e.db.Close()
		}
		e.dbMu.RUnlock()
		return fmt.Errorf("engine: final sync: %w", err)
	}
	e.dbMu.RLock()
	defer e.dbMu.RUnlock()
	return e.db.Close()
}
