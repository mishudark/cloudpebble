# CloudPebble Reliability Hardening Plan

## Overview

This document describes the reliability improvements identified during code review of `pkg/engine` and `pkg/walcloud`. Issues are sorted by severity (Critical → High → Medium → Minor).

---

## Critical Fixes

### 1. Local apply must follow WAL durability confirmation (engine.go:362-399)

**Problem:** `writeWALAndApply` applies the mutation to Pebble locally at line 376, then waits on the GCS durability `done` channel at line 383. If the GCS write fails, the caller receives an error but the local state has already diverged. The mutation is in Pebble with no durable backing, and there is no rollback path.

**Fix:** Swap the order — wait on GCS durability first, then apply locally. This ensures that if the write isn't durable, local state is never mutated.

```
// Before:
walMgr.WriteRecord → db.Apply → wait done

// After:
walMgr.WriteRecord → wait done → db.Apply
```

**Note:** This adds GCS round-trip latency to the hot path for non-batched writes. For batched writes, the latency is already amortized. Consider exposing the batched/non-batched mode to callers who need low latency vs. strong durability.

### 2. Guard Sync against concurrent calls (engine.go:555-706)

**Problem:** `Sync()` is called from three sites — `syncLoop` (periodic), `Close()` (final), and `checkEviction()` (overflow compaction) — with no mutual exclusion. Concurrent syncs race on:
- Creating/deleting `checkpointDir` (line 576-577)
- Mutating `e.uploadedFiles` (lines 635, 640-650)
- Writing manifest files (lines 680-686)
- Both could delete files the other just uploaded (stale cleanup at line 643)

**Fix:** Add a `sync.Mutex` (`syncMu`) to the Engine struct. `Sync()` acquires the lock at entry and holds it for the entire duration. All three call sites are serialized.

---

## High-Priority Fixes

### 3. Preserve user PebbleOptions through recovery (engine.go:279-281)

**Problem:** `recover()` opens Pebble with a bare `&pebble.Options{DisableWAL: true}` instead of the user's configured `opts.PebbleOptions`. This is called at both `Open()` (initial start) and on every cold recovery. Users who set custom `Comparer`, `Merger`, `CacheSize`, `MaxOpenFiles`, or `Levels` lose those settings on recovery. A cold-recovered node behaves differently from a healthy node.

**Fix:** Store the user's PebbleOptions on the Engine struct (`pebbleOpts *pebble.Options`) and use it in `recover()`, ensuring `DisableWAL: true` is still forced.

### 4. Fix TOCTOU race in Get during cold recovery (engine.go:451-476)

**Problem:** `Get()` captures `e.db` under `RLock`, releases the lock, then calls `db.Get(key)`. Meanwhile `recover()` (running in background goroutine via `triggerColdRecovery`) can:
1. Acquire `Lock` on `dbMu`
2. Close the old `e.db`
3. Open and assign a new `e.db`
4. Release `Lock` on `dbMu`

The `Get()` call on the old, now-closed `db` returns an error. This error is `!= pebble.ErrNotFound`, so it's NOT counted as a miss and the cold-miss counter isn't incremented — the caller just gets an opaque error. However, if `pebble.ErrNotFound` is returned (because the old DB is closing), it triggers additional cold-miss counting and could re-trigger recovery.

**Fix:** Check `e.recovering.Load()` before using the cached `db` reference. If recovering, either wait (retry with fresh `e.db`) or return a "retryable" error.

---

## Medium-Priority Fixes

### 5. Plumb cancellable context to WAL batch flush (walcloud/manager.go:158)

**Problem:** `flushPending` launches a goroutine that calls `store.Put(context.Background(), ...)`. On engine shutdown, `wg.Wait()` in `Close()` blocks forever if a GCS write is hung.

**Fix:** Give the Manager a `ctx` (set at construction time from the engine) and use it in `flushPending`'s goroutine.

### 6. Handle batch data when local Apply fails (engine.go:376 vs 366)

**Problem:** In the batched path, `walMgr.WriteRecord` appends data to `m.pending` at line 127 BEFORE `db.Apply` is called at line 376. If `db.Apply` fails, the data has already been added to the pending batch and WILL be flushed to GCS. The caller gets an error, but the data becomes durable — semantically confusing.

**Fix:** Two possible approaches:
- **A (simple):** Accept that data already in the pending batch is fine — it simply means the batch applies durably and will be replayed on recovery. The local apply failure is the ordering mismatch, not data loss. Log a warning but don't attempt to remove from the batch.
- **B (correct):** Remove the segment from `m.pending` if Apply fails. This requires the Manager to expose a removal method, which is complex given concurrent access.

Given the complexity of approach B and the fact that approach A doesn't cause actual data corruption (just a spurious error), we choose **approach A** with improved logging.

---

## Minor Fixes

### 7. Fix BatchWindow default mismatch (engine.go:93-96 vs 174)

**Problem:** Docstring says "Zero means use the default (1s)". Code uses 200ms.

**Fix:** Either update docstring to say 200ms or change code to 1s. 200ms is a reasonable default — update the docstring.

### 8. Eliminate dual file read in Sync (engine.go:626, 661)

**Problem:** Each checkpoint file is read from disk twice — once for upload (line 626) and once for the manifest checksum (line 661). Doubles disk I/O during sync.

**Fix:** Compute SHA256 hash during the upload pass and reuse the data buffer for the manifest. Read once, use twice.

### 9. Simplify isMutable (engine.go:601-608)

**Problem:** The `isMutable` helper iterates a map of prefixes with `strings.HasPrefix` on each invocation.

**Fix:** Replace with an inline check against known mutable file names. The set is small and static — a switch or simple string comparison suffices.

### 10. Add Close/Stop method to walcloud.Manager

**Problem:** The `time.AfterFunc` timer (line 125) leaks if the Manager is discarded before it fires. No cleanup method.

**Fix:** Add `Close()` that stops any pending timer and prevents new writes.

---

## Implementation Order

1. [Critical] Fix 1 — Reorder WAL durability + local apply in `writeWALAndApply`
2. [Critical] Fix 2 — Add `sync.Mutex` guard on `Sync()`
3. [High] Fix 3 — Preserve PebbleOptions in engine struct for recovery
4. [High] Fix 4 — Guard Get() with recovering flag
5. [Medium] Fix 5 — Cancellable context to Manager
6. [Medium] Fix 6 — Handle failed batch apply (log warning)
7. [Minor] Fixes 7-10 — Cleanup items
8. Run all tests

## Test Plan

After each fix:
- `go test ./pkg/engine/... ./pkg/walcloud/... -count=1`
- Fuzz tests: `go test -fuzz=. -fuzztime=10s ./pkg/engine/... ./pkg/walcloud/...`
- Race detector: `go test -race ./pkg/engine/... ./pkg/walcloud/...`
