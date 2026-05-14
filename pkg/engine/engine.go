// Package engine provides the top-level CloudPebble engine that combines a
// local Pebble instance (read cache) with durable object storage (GCS WAL +
// SSTs).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/mishudark/cloudpebble/pkg/objstore"
	"github.com/mishudark/cloudpebble/pkg/walcloud"
)

const (
	defaultSyncInterval = 30 * time.Second
	manifestName        = "manifest"
	dataDir             = "data"
	ckptDir             = "checkpoint"
)

// manifest is the metadata file stored in object storage that records the
// last checkpoint version and the highest WAL sequence number covered by
// that checkpoint.
type manifest struct {
	MaxWALSeq uint64 `json:"max_wal_seq"`
}

// Options configures an Engine.
type Options struct {
	// Dir is the local directory used for the Pebble instance cache.
	Dir string

	// Store is the object storage backend for durable data.
	Store objstore.Store

	// Namespace is the tenant/namespace identifier. Data is stored under
	// {namespace}/ in object storage.
	Namespace string

	// SyncInterval controls how often local SSTs are uploaded to object
	// storage. If zero, defaults to 30 seconds.
	SyncInterval time.Duration

	// PebbleOptions are passed through to the underlying Pebble instance.
	// The Engine forces DisableWAL to true regardless of this setting.
	PebbleOptions *pebble.Options
}

// Engine is the core CloudPebble instance for a single namespace. It
// provides a key-value API backed by a local Pebble cache with durable
// persistence in object storage.
type Engine struct {
	ns      string
	store   objstore.Store
	walMgr  *walcloud.Manager
	db      *pebble.DB
	localDir string

	mu        sync.Mutex
	maxWALSeq uint64

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
	if opts.PebbleOptions == nil {
		opts.PebbleOptions = &pebble.Options{}
	}
	opts.PebbleOptions.EnsureDefaults()
	opts.PebbleOptions.DisableWAL = true

	walMgr, err := walcloud.NewManager(opts.Store, opts.Namespace)
	if err != nil {
		return nil, fmt.Errorf("engine: creating WAL manager: %w", err)
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("engine: creating local dir: %w", err)
	}

	e := &Engine{
		ns:      opts.Namespace,
		store:   opts.Store,
		walMgr:  walMgr,
		localDir: opts.Dir,
	}

	if err := e.recover(context.Background(), opts); err != nil {
		return nil, fmt.Errorf("engine: recovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.ctx = ctx
	e.cancel = cancel

	go e.syncLoop(opts.SyncInterval)

	return e, nil
}

// recover downloads checkpoint data from object storage (if any) and replays
// uncommitted WAL entries.
func (e *Engine) recover(ctx context.Context, opts Options) error {
	manifestBytes, err := opts.Store.Get(ctx, e.manifestPath())
	hasManifest := err == nil

	if hasManifest {
		var m manifest
		if err := json.Unmarshal(manifestBytes, &m); err != nil {
			return fmt.Errorf("decoding manifest: %w", err)
		}

		dataPrefix := e.dataPrefix()
		files, err := opts.Store.List(ctx, dataPrefix)
		if err != nil {
			return fmt.Errorf("listing data files: %w", err)
		}
		for _, f := range files {
			localName := filepath.Base(f)
			localPath := filepath.Join(opts.Dir, localName)
			data, err := opts.Store.Get(ctx, f)
			if err != nil {
				return fmt.Errorf("downloading %s: %w", f, err)
			}
			if err := os.WriteFile(localPath, data, 0644); err != nil {
				return fmt.Errorf("writing local %s: %w", localPath, err)
			}
		}

		e.maxWALSeq = m.MaxWALSeq
	}

	db, err := pebble.Open(opts.Dir, opts.PebbleOptions)
	if err != nil {
		return fmt.Errorf("opening pebble: %w", err)
	}
	e.db = db

	if hasManifest {
		if err := e.replayWALs(ctx); err != nil {
			db.Close()
			return fmt.Errorf("replaying WALs: %w", err)
		}
	} else {
		if err := e.replayWALs(ctx); err != nil {
			db.Close()
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

// Set stores a key-value pair. The write is durably committed to object
// storage before returning.
func (e *Engine) Set(ctx context.Context, key, value []byte) error {
	batch := e.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, value, nil); err != nil {
		return fmt.Errorf("engine: building set: %w", err)
	}

	data := batch.Repr()

	seq, err := e.walMgr.WriteRecord(ctx, data)
	if err != nil {
		return fmt.Errorf("engine: writing WAL: %w", err)
	}

	if err := e.db.Apply(batch, pebble.NoSync); err != nil {
		return fmt.Errorf("engine: applying: %w", err)
	}

	e.mu.Lock()
	if seq > e.maxWALSeq {
		e.maxWALSeq = seq
	}
	e.mu.Unlock()

	return nil
}

// Get retrieves the value for a key.
func (e *Engine) Get(key []byte) ([]byte, error) {
	val, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(val))
	copy(result, val)
	closer.Close()
	return result, nil
}

// Delete removes a key. The deletion is durably committed to object storage
// before returning.
func (e *Engine) Delete(ctx context.Context, key []byte) error {
	batch := e.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(key, nil); err != nil {
		return fmt.Errorf("engine: building delete: %w", err)
	}

	data := batch.Repr()

	seq, err := e.walMgr.WriteRecord(ctx, data)
	if err != nil {
		return fmt.Errorf("engine: writing WAL: %w", err)
	}

	if err := e.db.Apply(batch, pebble.NoSync); err != nil {
		return fmt.Errorf("engine: applying: %w", err)
	}

	e.mu.Lock()
	if seq > e.maxWALSeq {
		e.maxWALSeq = seq
	}
	e.mu.Unlock()

	return nil
}

// Apply writes a pre-built batch durably. The batch is committed to object
// storage WAL before being applied locally.
func (e *Engine) Apply(ctx context.Context, batch *pebble.Batch) error {
	data := batch.Repr()

	seq, err := e.walMgr.WriteRecord(ctx, data)
	if err != nil {
		return fmt.Errorf("engine: writing WAL: %w", err)
	}

	if err := e.db.Apply(batch, pebble.NoSync); err != nil {
		return fmt.Errorf("engine: applying: %w", err)
	}

	e.mu.Lock()
	if seq > e.maxWALSeq {
		e.maxWALSeq = seq
	}
	e.mu.Unlock()

	return nil
}

// DB returns the underlying local Pebble instance. Callers may use this for
// advanced operations like iteration, snapshots, or custom compaction
// control.
func (e *Engine) DB() *pebble.DB {
	return e.db
}

// Sync flushes the local Pebble instance and uploads a consistent checkpoint
// to object storage. After Sync returns, all data up to the current point is
// durably stored in object storage as both WAL entries and SST files.
func (e *Engine) Sync(ctx context.Context) error {
	if err := e.db.Flush(); err != nil {
		return fmt.Errorf("engine: flush: %w", err)
	}

	checkpointDir := filepath.Join(e.localDir, ckptDir)
	os.RemoveAll(checkpointDir)
	defer os.RemoveAll(checkpointDir)

	if err := e.db.Checkpoint(checkpointDir); err != nil {
		return fmt.Errorf("engine: checkpoint: %w", err)
	}

	entries, err := os.ReadDir(checkpointDir)
	if err != nil {
		return fmt.Errorf("engine: reading checkpoint dir: %w", err)
	}

	e.mu.Lock()
	currentSeq := e.maxWALSeq
	dataPrefix := e.dataPrefix()

	existingFiles, err := e.store.List(ctx, dataPrefix)
	if err != nil {
		e.mu.Unlock()
		return fmt.Errorf("engine: listing existing data files: %w", err)
	}
	existingSet := make(map[string]bool, len(existingFiles))
	for _, f := range existingFiles {
		existingSet[filepath.Base(f)] = true
	}
	e.mu.Unlock()

	checkpointFiles := make(map[string]bool)
	for _, entry := range entries {
		checkpointFiles[entry.Name()] = true

		localPath := filepath.Join(checkpointDir, entry.Name())
		data, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("engine: reading checkpoint file %s: %w", entry.Name(), err)
		}
		remotePath := filepath.ToSlash(filepath.Join(dataPrefix, entry.Name()))
		if err := e.store.Put(ctx, remotePath, data); err != nil {
			return fmt.Errorf("engine: uploading %s: %w", entry.Name(), err)
		}
	}

	for f := range existingSet {
		if !checkpointFiles[f] {
			remotePath := filepath.ToSlash(filepath.Join(dataPrefix, f))
			if err := e.store.Delete(ctx, remotePath); err != nil {
				return fmt.Errorf("engine: deleting stale file %s: %w", f, err)
			}
		}
	}

	m := manifest{MaxWALSeq: currentSeq}
	manifestBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("engine: encoding manifest: %w", err)
	}
	if err := e.store.Put(ctx, e.manifestPath(), manifestBytes); err != nil {
		return fmt.Errorf("engine: writing manifest: %w", err)
	}

	if _, err := e.walMgr.GC(ctx, currentSeq); err != nil {
		return fmt.Errorf("engine: WAL GC: %w", err)
	}

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
		}
	}
}

func (e *Engine) manifestPath() string {
	return filepath.ToSlash(filepath.Join(e.ns, manifestName))
}

func (e *Engine) dataPrefix() string {
	return filepath.ToSlash(filepath.Join(e.ns, dataDir)) + "/"
}

// Close gracefully shuts down the engine, flushing data and uploading a final
// checkpoint.
func (e *Engine) Close() error {
	e.cancel()

	if err := e.Sync(context.Background()); err != nil {
		e.db.Close()
		return fmt.Errorf("engine: final sync: %w", err)
	}
	return e.db.Close()
}
