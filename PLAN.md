# Implementation Plan: From Shortcuts to Production

This document describes the concrete implementation plan for each
identified shortcut in the v0.1 CloudPebble prototype. Items are
listed in priority order (P0 = correctness/safety, P4 = scale/nice-to-have).

---

## P0-1: Cold Miss Fallback

**Problem:** `Get()` only checks local Pebble. On a cold node (new
process, empty cache), all reads return `ErrNotFound` even though the
data exists in GCS SSTs and WALs. A cold node is effectively blind
until the first `Sync()` downloads the checkpoint — but `Sync()` runs
periodically, so the window could be minutes.

**Solution: Add a threshold-based one-shot recovery trigger.**

### Implementation

**`engine/options.go`** — new options:

```go
// ColdMissThreshold is the number of consecutive cache misses before
// the engine triggers a one-shot recovery download from object storage.
// Zero disables automatic recovery (cold nodes will return ErrNotFound).
// Default: 3.
ColdMissThreshold int
```

**`engine/engine.go`** — new fields:

```go
coldMissCount   atomic.Int64  // consecutive misses since last hit
recovering      atomic.Bool   // prevents concurrent recoveries
```

**`engine/engine.go`** — modify `Get()`:

```go
func (e *Engine) Get(key []byte) ([]byte, error) {
    val, closer, err := e.db.Get(key)
    if err == nil {
        e.coldMissCount.Store(0)
        // ... return value
    }
    if err == pebble.ErrNotFound && e.coldMissThreshold > 0 {
        e.coldMissCount.Add(1)
        if e.coldMissCount.Load() >= int64(e.coldMissThreshold) {
            e.triggerColdRecovery()
        }
        return nil, err
    }
    return nil, err
}
```

**`engine/engine.go`** — new method `triggerColdRecovery()`:

```go
func (e *Engine) triggerColdRecovery() {
    if !e.recovering.CompareAndSwap(false, true) {
        return // already recovering
    }
    go func() {
        defer e.recovering.Store(false)
        // Close current Pebble (drain writes first)
        // Download latest data/ files from GCS
        // Replay WALs from GCS
        // Reopen Pebble
        // Reset coldMissCount
    }()
}
```

The recovery reuses the existing `recover()` logic extracted into a
standalone method that downloads checkpoint + replays WALs.

### Files changed
- `pkg/engine/engine.go` — modify `Get()`, add `triggerColdRecovery()`, extract `recover()` into reusable method
- `pkg/engine/options.go` — add `ColdMissThreshold`

### Verification
- Delete local Pebble dir after writes
- `Get()` should trigger recovery and return data
- Concurrent `Get()` calls during recovery should be served once recovery completes

---

## P0-2: Incremental SST Upload

**Problem:** `Sync()` uploads the entire Pebble checkpoint to GCS every
30 seconds. A 100GB database re-uploads 100GB every sync cycle.

**Solution: Track which files are already in GCS, upload only new ones.**

### Implementation

**`engine/engine.go`** — add field:

```go
uploadedFiles   map[string]struct{}  // remote paths already in GCS data/ dir
```

**`engine/engine.go`** — modify `Sync()`:

```go
func (e *Engine) Sync(ctx context.Context) error {
    // 1. Flush + checkpoint (unchanged)
    // 2. List checkpoint entries (unchanged)
    // 3. For each entry:
    for _, entry := range entries {
        remoteName := filepath.ToSlash(filepath.Join(dataPrefix, entry.Name()))
        if _, ok := e.uploadedFiles[remoteName]; ok {
            continue // already in GCS
        }
        // upload file to GCS
        e.uploadedFiles[remoteName] = struct{}{}
    }
    // 4. Delete stale files from GCS (files in uploadedFiles but not in checkpoint):
    for remoteName := range e.uploadedFiles {
        if !checkpointFiles[filepath.Base(remoteName)] {
            e.store.Delete(ctx, remoteName)
            delete(e.uploadedFiles, remoteName)
        }
    }
    // 5. Write manifest + GC WALs (unchanged)
}
```

**`engine/engine.go`** — modify `recover()` to populate `uploadedFiles`:

```go
// After downloading data/ files:
for _, f := range files {
    remoteName := filepath.ToSlash(filepath.Join(dataPrefix, filepath.Base(f)))
    e.uploadedFiles[remoteName] = struct{}{}
}
```

### Files changed
- `pkg/engine/engine.go` — modify `Sync()` and `recover()`, add `uploadedFiles` field

### Verification
- Run Sync twice, check that second upload only transmits new files
- Delete a local SST (simulating compaction), verify next Sync deletes it from GCS

---

## P1-1: Eventual Consistency

**Problem:** Always strong consistency. On cold start, ALL unflushed
WALs must be replayed before the node can serve reads. For large WAL
backlogs, this extends startup latency from ~seconds to minutes.

**Solution: Allow optional eventually-consistent mode where WAL replay
is skipped on open, and the node serves slightly stale data immediately.**

### Implementation

**`engine/options.go`** — new types:

```go
type ConsistencyLevel int

const (
    ConsistencyStrong   ConsistencyLevel = iota // replay WALs on open (default)
    ConsistencyEventual                        // skip WAL replay, serve from last checkpoint
)
```

**`engine/engine.go`** — add field:

```go
consistency ConsistencyLevel
```

**`engine/engine.go`** — modify `recover()`:

```go
func (e *Engine) recover(ctx context.Context, opts Options) error {
    // ... download checkpoint (unchanged) ...

    db, err := pebble.Open(opts.Dir, opts.PebbleOptions)
    e.db = db

    if e.consistency == ConsistencyStrong {
        // current behavior: replay all WALs > maxWALSeq
        if err := e.replayWALs(ctx); err != nil {
            return err
        }
    }
    // For ConsistencyEventual: skip replay, serve from checkpoint only.
    // The background sync loop will eventually catch up by uploading
    // new SSTs and advancing maxWALSeq.
    return nil
}
```

**`engine/engine.go`** — modify `syncLoop()`:

When consistency is `Eventual`, the periodic `Sync()` call already does:
- Flush → checkpoint → upload SSTs → advance maxWALSeq → GC WALs
This self-heals: each `Sync()` cycle catches the node up to the latest
durable state, shrinking the staleness window.

### Files changed
- `pkg/engine/options.go` — add `ConsistencyLevel`
- `pkg/engine/engine.go` — add field, modify `recover()`, modify `syncLoop()`

### Verification
- Write data, Sync (upload checkpoint), write more data, kill process
- Open with `Eventual` → immediately serve first batch but not second
- Wait for next Sync → second batch becomes visible

---

## P1-2: Orphan WAL Cleanup

**Problem:** If a GCS WAL write succeeds but the local Pebble `Apply`
fails, the WAL object stays in GCS forever. GC only deletes WALs with
`seq ≤ maxWALSeq`, and `maxWALSeq` only advances on fully successful
writes. Failed writes produce orphan WALs that accumulate over time.

**Solution: Track WAL object age via GCS metadata. Delete objects older
than a threshold that have seq > maxWALSeq (meaning they were never
applied successfully).**

### Implementation

**`objstore/store.go`** — add `Attrs` to the interface (needed for object metadata):

```go
type ObjectInfo struct {
    Path      string
    CreatedAt time.Time
}

type Store interface {
    // ... existing methods ...
    Attrs(ctx context.Context, path string) (ObjectInfo, error)  // NEW
}
```

**`objstore/gcs/gcs.go`** — implement `Attrs`:

```go
func (s *Store) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
    attrs, err := s.client.Bucket(s.bucket).Object(s.fullPath(path)).Attrs(ctx)
    if err != nil {
        return objstore.ObjectInfo{}, err
    }
    return objstore.ObjectInfo{
        Path:      path,
        CreatedAt: attrs.Created,
    }, nil
}
```

**`objstore/local/local.go`** — implement `Attrs` using os.Stat + ModTime:

```go
func (s *Store) Attrs(ctx context.Context, path string) (objstore.ObjectInfo, error) {
    fi, err := os.Stat(s.path(path))
    if err != nil {
        return objstore.ObjectInfo{}, err
    }
    return objstore.ObjectInfo{
        Path:      path,
        CreatedAt: fi.ModTime(),
    }, nil
}
```

**`walcloud/manager.go`** — modify `GC()`:

```go
func (m *Manager) GC(ctx context.Context, maxSeq uint64, orphanTTL time.Duration) (deleted int, err error) {
    entries, err := m.listWALs(ctx)
    for _, e := range entries {
        if e.Seq <= maxSeq {
            // Normal GC: WAL data is in an uploaded SST
            m.store.Delete(ctx, e.Path)
            deleted++
            continue
        }
        if orphanTTL <= 0 {
            continue // no orphan cleanup configured
        }
        // Check object age for orphan detection
        info, err := m.store.Attrs(ctx, e.Path)
        if err != nil {
            continue // can't determine age, skip
        }
        if time.Since(info.CreatedAt) > orphanTTL {
            m.store.Delete(ctx, e.Path)
            deleted++
        }
    }
}
```

This is safe because: if a WAL entry has `seq > maxWALSeq` AND was
created more than `orphanTTL` ago, the engine has had plenty of time
to apply it. If it hasn't been applied by now, the write failed and
the WAL is orphaned.

**`engine/engine.go`** — pass `orphanTTL` through from options to `walMgr.GC()`.
Default: 1 hour.

### Files changed
- `pkg/objstore/store.go` — add `Attrs` method and `ObjectInfo` struct
- `pkg/objstore/gcs/gcs.go` — implement `Attrs`
- `pkg/objstore/local/local.go` — implement `Attrs`
- `pkg/walcloud/manager.go` — modify `GC()`, add `orphanTTL` parameter
- `pkg/engine/engine.go` — pass through orphanTTL from options

### Verification
- Simulate a failed Apply (write WAL, don't apply)
- Wait for orphanTTL, trigger GC, verify orphan WAL is deleted
- Verify normal WALs (seq ≤ maxWALSeq) are still GC'd

---

## P2-1: WAL Batching (Write Throughput)

**Problem:** Each write creates a separate GCS WAL object. This means
one GCS network round-trip per write (~100ms). Write throughput is
bounded by GCS latency (~10 writes/sec), not by I/O bandwidth.

**Solution: Batch concurrent writes within a configurable time window
into a single GCS WAL object. Analogous to turbopuffer's "1 WAL entry
per second" model.**

### Implementation

**`walcloud/manager.go`** — restructure for batching:

```go
type Manager struct {
    store  objstore.Store
    ns     string

    mu         sync.Mutex
    nextSeq    uint64

    // Batching
    batchWindow  time.Duration   // e.g. 1s, 0 = no batching (one WAL per write)
    pendingBatch *pebble.Batch   // nil when no pending batch
    pendingSeq   uint64          // seq assigned to pendingBatch
    pendingC     chan struct{}   // closed when the batch is committed to GCS
    commitTimer  *time.Timer

    ctx    context.Context
    cancel context.CancelFunc
}
```

**Write path:**

```go
func (m *Manager) WriteRecord(ctx context.Context, batch *pebble.Batch) (seq uint64, commit chan struct{}, err error) {
    if m.batchWindow == 0 {
        // No batching: direct write (current behavior)
        return m.writeDirect(ctx, batch)
    }

    m.mu.Lock()
    defer m.mu.Unlock()

    if m.pendingBatch == nil {
        m.pendingSeq = m.nextSeq
        m.nextSeq++
        m.pendingBatch = batch
        m.pendingC = make(chan struct{})
        m.commitTimer = time.AfterFunc(m.batchWindow, m.flushPending)
    } else {
        // Append operations to pending batch
        for _, op := range batch.Operations() { ... }
        batch.Close() // caller's batch consumed
    }
    return m.pendingSeq, m.pendingC, nil
}

func (m *Manager) flushPending() {
    m.mu.Lock()
    batch := m.pendingBatch
    seq := m.pendingSeq
    m.pendingBatch = nil
    m.mu.Unlock()

    if batch == nil {
        return
    }

    data := batch.Repr()
    batch.Close()

    m.store.Put(context.Background(), m.walPath(seq), data)
    close(m.pendingC) // wake all waiters
}
```

**`engine/engine.go`** — updated write methods:

```go
func (e *Engine) Set(ctx context.Context, key, value []byte) error {
    batch := e.db.NewBatch()
    batch.Set(key, value, nil)

    seq, commitC, err := e.walMgr.WriteRecord(ctx, batch)
    if err != nil {
        batch.Close()
        return err
    }

    // Apply to local Pebble immediately (write is visible before WAL commit)
    if err := e.db.Apply(batch, pebble.NoSync); err != nil {
        batch.Close()
        return err
    }

    // Wait for GCS WAL durability
    if commitC != nil {
        select {
        case <-commitC:
        case <-ctx.Done():
            return ctx.Err()
        }
    }

    e.mu.Lock()
    if seq > e.maxWALSeq {
        e.maxWALSeq = seq
    }
    e.mu.Unlock()
    return nil
}
```

### Durability semantics

With batching enabled (`batchWindow > 0`):
- Writes are acknowledged only after the GCS WAL write completes.
- Multiple writes within `batchWindow` share one GCS write (amortized latency).
- If the process crashes during the window, all pending writes are lost
  (they're in memory only). **Callers must provide idempotency at the
  application level.**

### Files changed
- `pkg/walcloud/manager.go` — full rewrite of `WriteRecord`, add `flushPending`, batching fields
- `pkg/engine/engine.go` — modify `Set`/`Delete`/`Apply` to use new `WriteRecord` signature

### Verification
- Set 10 keys rapidly, verify only 1 GCS object created (when window > total write time)
- Crash between batch creation and GCS write, verify data loss within the window
- Verify with `batchWindow=0` (no batching), behavior matches current

---

## P2-2: Metrics / Logging

**Problem:** Zero observability. No latency histograms, no throughput
counters, no error rates.

**Solution: Track per-operation metrics and expose them via a struct.
Use Pebble's existing `EventListener` to surface internal stats.**

### Implementation

**`engine/metrics.go`** — new file:

```go
package engine

import (
    "sync/atomic"
    "time"
)

type EngineMetrics struct {
    Sets          atomic.Int64
    Gets          atomic.Int64
    GetMisses     atomic.Int64  // cache misses (ErrNotFound)
    Deletes       atomic.Int64

    // Cumulative latencies in nanoseconds (for computing averages)
    WALWriteLatencyNs  atomic.Int64
    ApplyLatencyNs     atomic.Int64
    SyncLatencyNs      atomic.Int64

    // Snapshot counters
    WALObjectsGCd      atomic.Int64
    SyncFailures       atomic.Int64
    ColdRecoveries     atomic.Int64
}

func (m *EngineMetrics) AverageWALWriteLatency() time.Duration {
    n := m.Sets.Load() + m.Deletes.Load()
    if n == 0 {
        return 0
    }
    return time.Duration(m.WALWriteLatencyNs.Load() / n)
}
```

**`engine/engine.go`** — add field:

```go
metrics EngineMetrics
```

**Modify `Set()`:**

```go
start := time.Now()
seq, err := e.walMgr.WriteRecord(ctx, data)
e.metrics.WALWriteLatencyNs.Add(time.Since(start).Nanoseconds())
e.metrics.Sets.Add(1)
```

**Modify `Sync()`:**

```go
start := time.Now()
// ... do sync ...
if err != nil {
    e.metrics.SyncFailures.Add(1)
}
e.metrics.SyncLatencyNs.Add(time.Since(start).Nanoseconds())
```

**Expose:**

```go
func (e *Engine) Metrics() *EngineMetrics {
    return &e.metrics
}
```

### Files changed
- `pkg/engine/metrics.go` — new file
- `pkg/engine/engine.go` — add metrics field, wire into operations

---

## P3-1: Robust Manifest

**Problem:** Current manifest is `{"max_wal_seq": N}` — no file
inventory, no checksums, no version history. Corrupted SST files in
GCS go undetected. Partial uploads (process crash during Sync) leave
inconsistent GCS state.

**Solution: Versioned manifests with file metadata and SHA-256
checksums. Keep manifest history for rollback.**

### Implementation

**`engine/manifest.go`** — new file:

```go
package engine

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "time"
)

type Manifest struct {
    Version     int              `json:"version"`    // monotonic
    MaxWALSeq   uint64           `json:"max_wal_seq"`
    CreatedAt   time.Time        `json:"created_at"`
    PrevVersion int              `json:"prev_version"` // 0 on first version
    Files       []ManifestFile   `json:"files"`
}

type ManifestFile struct {
    Name     string `json:"name"`     // "000004.sst"
    Size     int64  `json:"size"`
    Checksum string `json:"checksum"` // SHA-256 hex
}
```

**`engine/engine.go`** — modify `Sync()`:

```go
// After uploading checkpoint files:
var mf Manifest
mf.Version = e.manifestVersion + 1
mf.PrevVersion = e.manifestVersion
mf.MaxWALSeq = currentSeq
mf.CreatedAt = time.Now()

for _, entry := range entries {
    localPath := filepath.Join(checkpointDir, entry.Name())
    data, _ := os.ReadFile(localPath)
    hash := sha256.Sum256(data)
    mf.Files = append(mf.Files, ManifestFile{
        Name:     entry.Name(),
        Size:     int64(len(data)),
        Checksum: hex.EncodeToString(hash[:]),
    })
}

manifestBytes, _ := json.Marshal(mf)
// Write to {ns}/manifest (current)
e.store.Put(ctx, e.manifestPath(), manifestBytes)
// Also write to {ns}/manifests/{version}.json (history)
e.store.Put(ctx, e.manifestVersionPath(mf.Version), manifestBytes)

// Keep last N manifests (default 10), delete older ones
mfs, _ := e.store.List(ctx, e.manifestVersionsPrefix())
if len(mfs) > e.manifestHistoryLimit {
    for _, old := range mfs[:len(mfs)-e.manifestHistoryLimit] {
        e.store.Delete(ctx, old)
    }
}

e.manifestVersion = mf.Version
```

**`engine/engine.go`** — modify `recover()`:

```go
manifestBytes, err := opts.Store.Get(ctx, e.manifestPath())
// Parse Manifest struct
// For each file in Manifest.Files:
//   - If file already exists locally, verify checksum
//   - If checksum mismatches or file missing, download from GCS
// After download: verify all checksums match before opening Pebble
```

**Rollback support** (optional, P3):

```go
func (e *Engine) RollbackToVersion(v int) error {
    mfBytes, _ := e.store.Get(ctx, e.manifestVersionPath(v))
    var mf Manifest
    json.Unmarshal(mfBytes, &mf)
    // Download all files from that version
    // Delete newer files locally + in GCS
    // Reopen Pebble
}
```

### Files changed
- `pkg/engine/manifest.go` — new file with `Manifest` type + helpers
- `pkg/engine/engine.go` — modify `Sync()` and `recover()`, add version tracking

---

## P3-2: Non-blocking Sync

**Problem:** `Sync()` calls `db.Flush()` and `db.Checkpoint()` which
hold Pebble's internal `d.mu`, blocking ALL concurrent writes during
the sync. With incremental upload, the GCS upload part is fast, but
Flush+Checkpoint still pauses writes for the duration of the memtable
flush + file copy.

**Solution: Use async flush, minimize checkpoint window.**

### Implementation

**`engine/engine.go`** — modify `Sync()`:

```go
func (e *Engine) Sync(ctx context.Context) error {
    // Step 1: Async flush — non-blocking, returns immediately
    flushDone, err := e.db.AsyncFlush()
    if err != nil {
        return err
    }
    <-flushDone // wait for flush to finish (new writes can continue alongside)

    // Step 2: Checkpoint — this blocks writes briefly but only for file copy
    // since flush already committed all memtable data to SSTs
    checkpointDir := filepath.Join(e.localDir, ckptDir)
    os.RemoveAll(checkpointDir)
    defer os.RemoveAll(checkpointDir)

    if err := e.db.Checkpoint(checkpointDir); err != nil {
        return err
    }
    // Writes can resume here

    // Step 3: Upload checkpoint files (async from Pebble's perspective)
    // ... incremental upload (P0-2) ...
}
```

The key change: replace `db.Flush()` (blocking) with `db.AsyncFlush()` +
wait. This lets new writes proceed into the next memtable while the
current one is being flushed to SSTs.

### Files changed
- `pkg/engine/engine.go` — modify `Sync()` to use `AsyncFlush()`

### Verification
- Run concurrent writes during Sync, verify no write latency spikes
- Verify checkpoint data is consistent (all writes before flush are in SSTs)

---

## P4: Eviction / Space Management

**Problem:** Local Pebble grows unbounded. No limit on cache size.
A node with limited NVMe will eventually run out of disk space.

**Solution: Track local directory size. When over limit, compact older
SSTs and remove them from local disk if already uploaded to GCS.**

### Implementation

**`engine/options.go`** — add:

```go
// MaxLocalBytes is the soft limit on local Pebble cache size in bytes.
// When exceeded, the engine will attempt to evict older SSTs that have
// already been uploaded to GCS. Zero means no limit. Default: 0.
MaxLocalBytes int64
```

**`engine/engine.go`** — modify `syncLoop()` to add eviction check:

```go
func (e *Engine) syncLoop(interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-e.ctx.Done():
            return
        case <-ticker.C:
            e.Sync(context.Background())
            if e.maxLocalBytes > 0 {
                e.evictIfNeeded()
            }
        }
    }
}
```

**`engine/engine.go`** — `evictIfNeeded()`:

```go
func (e *Engine) evictIfNeeded() {
    // Check current size
    m := e.db.Metrics()
    currentSize := m.DiskSpaceUsage()

    if currentSize <= uint64(e.maxLocalBytes) {
        return
    }

    // Strategy: find the oldest L6 SST files that are uploaded to GCS,
    // compact them into fewer files, then delete the old ones from local disk.

    // For v1: trigger a full compaction. This reduces file count and
    // merges data. The next Sync will detect the new compacted files
    // and upload them. Old files will be detected as stale and deleted
    // from GCS.
    e.db.Compact(nil, nil, true)

    // Re-sync after compaction
    e.Sync(context.Background())
}
```

**Future improvement**: instead of full compaction, use `db.Excise()`
to surgically remove specific SST files that are already in GCS, then
re-download them on demand (cold miss path from P0-1).

### Files changed
- `pkg/engine/engine.go` — add `maxLocalBytes` field, `evictIfNeeded()`, modify `syncLoop()`
- `pkg/engine/options.go` — add `MaxLocalBytes`

---

## Implementation Order Summary

| Step | Item | Version | Effort | Impact |
|------|------|---------|--------|--------|
| 1 | Cold miss fallback | P0 | Medium | Correctness |
| 2 | Incremental SST upload | P0 | Small | Scalability |
| 3 | Eventual consistency | P1 | Small | Latency |
| 4 | Orphan WAL cleanup | P1 | Medium | Safety |
| 5 | WAL batching | P2 | Large | Throughput |
| 6 | Metrics / logging | P2 | Small | Observability |
| 7 | Robust manifest | P3 | Medium | Safety |
| 8 | Non-blocking sync | P3 | Small | Latency |
| 9 | Eviction | P4 | Medium | Scalability |
