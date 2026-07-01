# Performance Optimization Opportunities for CloudPebble

## 1. Engine — Sync Path (High Impact)

### 1.1 Parallel Checkpoint File Uploads
**File:** `pkg/engine/engine.go:~620-650`
**Problem:** `Sync()` uploads SST files to object storage **sequentially** in a for-loop. For a database with many SST files, this is N × GCS-latency.
**Fix:** Upload files concurrently with a worker pool (e.g., 4–8 parallel `store.Put` calls). The `syncMu` lock scope can be narrowed — only the metadata map mutation needs locking, not the network I/O.

### 1.2 Parallel SST Downloads During Recovery
**File:** `pkg/engine/engine.go:~260-275` (`recover()`)
**Problem:** Each manifest file is downloaded sequentially via `e.store.Get()`. Cold-start recovery time scales linearly with SST count.
**Fix:** Download files concurrently using `errgroup` with a concurrency limiter. A 100-file checkpoint going from sequential (~50s at 500ms/file) to 8-way parallel (~6s) is an 8× improvement.

### 1.3 Eliminate Dual File Read in Sync
**File:** `pkg/engine/engine.go:~626 & ~661`
**Problem:** Each checkpoint file is `os.ReadFile`'d twice — once for upload, once for SHA-256 checksum.
**Fix:** Compute the hash during the upload pass. Read once, hash while uploading:
```go
h := sha256.New()
// tee the data into the hasher while uploading
```

### 1.4 Stream Large SST Files Instead of Full Buffer
**File:** `pkg/engine/engine.go:~626`
**Problem:** `os.ReadFile` loads each SST fully into memory before uploading. For multi-GB SSTs, this causes memory spikes.
**Fix:** Use `store.PutReader()` with an `io.TeeReader` for hashing, streaming the file to object storage without full buffering.

### 1.5 Narrow `syncMu` Lock Scope
**File:** `pkg/engine/engine.go:~560`
**Problem:** `syncMu.Lock()` is held for the **entire** Sync duration — including all GCS network round-trips. This blocks `Close()` and any manual `Sync()` calls.
**Fix:** Lock only around the critical metadata sections (manifest write, `uploadedFiles` map updates). Release during file uploads and downloads.

---

## 2. Engine — Write Path (High Impact)

### 2.1 Batch Pool for Set/Delete
**File:** `pkg/engine/engine.go:~395-420` (`Set`, `Delete`)
**Problem:** Every `Set()` and `Delete()` call allocates a new `pebble.Batch`, applies one operation, then closes it. Under high write throughput, this creates massive GC pressure.
**Fix:** Use `sync.Pool` for batch reuse, or better — allow callers to batch multiple operations into a single `Apply()` call (the `Apply` method already exists but isn't exposed at the Bigtable level for single-row mutations).

### 2.2 Avoid `batch.Repr()` Copy in Batching Path
**File:** `pkg/walcloud/manager.go:~147`
**Problem:** `WriteRecord` copies the batch repr into a new buffer for every write in the batching path (`buf := make([]byte, len(data)); copy(buf, data)`). This is necessary because the caller may reuse the batch, but the copy could use a `sync.Pool` for the buffers.
**Fix:** Pool the copy buffers, or document that the caller must not reuse the batch until the done channel fires (enabling zero-copy).

### 2.3 Reduce Write Latency: Apply Before WAL Confirmation (AsyncWAL for All)
**File:** `pkg/engine/engine.go:~340-380`
**Problem:** In the synchronous write path, the engine waits for GCS WAL durability (`<-done`) **before** applying to local Pebble. This adds the full GCS round-trip to every write's critical path.
**Fix:** The `AsyncWAL` mode already solves this for persistent SSD deployments. For non-AsyncWAL, consider applying to Pebble concurrently with the WAL upload, then returning success only when both complete. This overlaps local apply latency with GCS latency instead of serializing them.

---

## 3. WAL Manager (Medium Impact)

### 3.1 Reuse Merge Buffer via sync.Pool
**File:** `pkg/walcloud/manager.go:~222` (`mergeBatchSegments`)
**Problem:** Every batch flush allocates a new `[]byte` for the merged result.
**Fix:** Use a `sync.Pool` for merge buffers, returning them after the GCS Put completes.

### 3.2 Dedicated Flush Goroutine Instead of `go func()` per Flush
**File:** `pkg/walcloud/manager.go:~178`
**Problem:** `flushPending()` spawns a new goroutine for every batch window expiry. Under high load, this creates many short-lived goroutines.
**Fix:** Use a dedicated flush worker goroutine that reads from a channel, or use the `flushMu` to serialize naturally.

### 3.3 Parallel WAL Deletes in GC
**File:** `pkg/walcloud/manager.go:~262-280`
**Problem:** `GC()` deletes WAL objects one at a time. After a Sync that advances `maxWALSeq` significantly, many WALs need deletion.
**Fix:** Delete in parallel with a small worker pool (4–8 concurrent deletes).

### 3.4 Avoid Full Sort in `listWALs` for GC
**File:** `pkg/walcloud/manager.go:~246-260`
**Problem:** `listWALs()` sorts all entries by sequence number. `GC()` only needs to identify entries with `seq <= maxSeq` — it doesn't need a total order.
**Fix:** For GC, skip the sort and just filter. For `List()` (used in recovery), keep the sort.

---

## 4. Bigtable — ReadRows Path (Medium-High Impact)

### 4.1 Single-Allocation `EncodeCellKey`
**File:** `pkg/bigtable/encoding.go:~120-126`
**Problem:** `EncodeCellKey` calls `encodeRowPrefix` → `encodeFamilyPrefix` → `encodeColumnPrefix`, each allocating a new `[]byte`, then a 4th allocation for the final key. That's 4 allocations per cell key on the write path.
**Fix:** Compute the total length upfront and do a single `make([]byte, totalLen)`, writing all components into it:
```go
func EncodeCellKey(rowKey []byte, family string, qualifier []byte, ts int64) []byte {
    // compute escaped row key length, total size, single allocation
    ...
}
```

### 4.2 Avoid Value Copy in `cellChunk`
**File:** `pkg/bigtable/readrows.go:~200-210`
**Problem:** `cellChunk()` copies `value`, `rowKey`, and `qualifier` into new slices for every cell. During a full-table scan of 10K rows, this is 10K+ allocations.
**Fix:** Since the `ReadRowsResponse` is serialized and sent immediately after flush, the iterator's value is still valid. Could reference the iterator's bytes directly and let gRPC's protobuf serializer copy them during marshaling. This requires careful lifetime management but eliminates one copy layer.

### 4.3 Double-Buffering for Chunk Flush
**File:** `pkg/bigtable/readrows.go:~130-140`
**Problem:** `flush()` does `append([]*...CellChunk(nil), chunkBuf...)` to copy the slice header for the response, then resets `chunkBuf`. This allocates a new backing array each flush.
**Fix:** Use two pre-allocated buffers and swap them — fill buffer A while buffer B is being sent.

### 4.4 Iterator Reuse Across Scan Ranges
**File:** `pkg/bigtable/readrows.go:~90-100`
**Problem:** For row-key-list scans, a new `pebble.Iterator` is created and closed for each range. Iterator creation involves memtable and SST merging, which is expensive.
**Fix:** For adjacent or overlapping ranges, merge them into fewer iterators. For point lookups (row key list), consider using `db.Get()` with key prefix bounds instead of full iterators.

---

## 5. Bigtable — Filter Engine (Low-Medium Impact)

### 5.1 Replace `strings.Compare` with `bytes.Compare`
**File:** `pkg/bigtable/filter.go:~195, ~330, ~370`
**Problem:** `columnRangeFilter`, `valueRangeFilter` use `strings.Compare(string(cell.qualifier), string(...))` which incurs string conversions and UTF-8-aware comparison for raw byte data.
**Fix:** Use `bytes.Compare(cell.qualifier, c.startQualifier)` — no string conversion, faster comparison.

### 5.2 Avoid String Allocation in `cellsPerColumnLimitFilter`
**File:** `pkg/bigtable/filter.go:~273`
**Problem:** `col := cell.family + "\x00" + string(cell.qualifier)` creates a new string allocation for every cell evaluation.
**Fix:** Use a `map[uint64]int` with a hash of family+qualifier, or use a nested map `map[string]map[string]int` to avoid concatenation.

### 5.3 Avoid Map Allocation in `interleaveFilter`
**File:** `pkg/bigtable/filter.go:~120`
**Problem:** `interleaveFilter.evaluate()` allocates a `cellIdentity` struct and inserts into a `map[cellIdentity]bool` for every cell. The map is lazily initialized.
**Fix:** Pre-allocate the map in `buildInterleave`. Consider a smaller key representation (e.g., hash-based).

---

## 6. Bigtable — Mutation Path (Low-Medium Impact)

### 6.1 Cache `time.Now().UnixMicro()` Per Batch
**File:** `pkg/bigtable/mutate.go:~176`
**Problem:** `applySetCell` calls `time.Now().UnixMicro()` for every `SetCell` with `TimestampMicros == -1`. For a `MutateRows` with 100 entries × 10 cells, that's 1000 syscalls.
**Fix:** Compute the timestamp once per `MutateRow`/`MutateRows` call and pass it through.

### 6.2 Per-Row Lock Map Never Cleans Up
**File:** `pkg/bigtable/server.go:~34` (`tableState.rowLocks`)
**Problem:** `rowLocks` is a `sync.Map` that accumulates entries for every unique row key ever locked via `CheckAndMutateRow`. For workloads with many distinct rows, this is an unbounded memory leak.
**Fix:** Use a sharded map with TTL-based eviction, or use a `sync.Map` with periodic cleanup of unlocked entries.

### 6.3 `ReadModifyWriteRow` Creates Iterator Per Rule
**File:** `pkg/bigtable/readmodifywrite.go:~115`
**Problem:** `readCellValue()` creates a new iterator for each rule. For a request with N rules on different columns, this is N iterator creations.
**Fix:** Create a single iterator over the row's key range and seek to each column, or batch reads for columns in the same family.

---

## 7. Bigtable — Session/VRPC (Low Impact)

### 7.1 Sequential Proto Unmarshal in `dispatchVRPC`
**File:** `pkg/bigtable/session.go:~80-120`
**Problem:** `dispatchVRPC` tries `proto.Unmarshal` for 6 different request types sequentially. Each failed unmarshal still parses partial fields before failing.
**Fix:** Use a oneof discriminator or examine the first few bytes of the payload to determine the message type before unmarshaling. Alternatively, use `proto.Unmarshal` with a probe message that has all fields optional.

---

## 8. Object Store (Low-Medium Impact)

### 8.1 Local Store: Avoid Full Directory Walk for List
**File:** `pkg/objstore/local/local.go:~95`
**Problem:** `List()` uses `filepath.WalkDir` to walk the entire directory tree, then sorts all results. For large local object stores (e.g., many WAL files), this is O(n log n) on every call.
**Fix:** For prefix queries that map to a single directory (common case), use `os.ReadDir` on that specific directory instead of walking the full tree.

### 8.2 GCS Store: Use `PutReader` for Large SST Uploads
**File:** `pkg/objstore/gcs/gcs.go:~53`
**Problem:** `Put()` buffers the entire object in memory via `w.Write(data)`. For large SST files (100MB+), this causes memory pressure.
**Fix:** For objects above a size threshold, use `PutReader()` with `bytes.NewReader(data)` to enable streaming, or refactor to always use `PutReader`.

---

## Summary by Impact vs Effort

| # | Optimization | Impact | Effort | Component |
|---|---|---|---|---|
| 1.2 | Parallel SST downloads in recovery | **High** | Small | engine |
| 1.1 | Parallel SST uploads in Sync | **High** | Medium | engine |
| 2.1 | Batch pool for Set/Delete | **High** | Small | engine |
| 4.1 | Single-allocation EncodeCellKey | **High** | Small | bigtable |
| 2.3 | Overlap WAL upload with local apply | **High** | Medium | engine |
| 1.3 | Eliminate dual file read in Sync | Medium | Small | engine |
| 1.5 | Narrow syncMu lock scope | Medium | Medium | engine |
| 3.3 | Parallel WAL deletes in GC | Medium | Small | walcloud |
| 4.3 | Double-buffering for chunk flush | Medium | Small | bigtable |
| 5.1 | bytes.Compare instead of strings.Compare | Low-Med | Trivial | bigtable |
| 6.1 | Cache timestamp per batch | Low-Med | Trivial | bigtable |
| 6.2 | Clean up per-row lock map | Medium | Small | bigtable |
| 3.1 | Pool merge buffers | Low | Small | walcloud |
| 1.4 | Stream large SSTs | Medium | Medium | engine |
| 4.2 | Avoid value copy in cellChunk | Medium | Medium | bigtable |
| 7.1 | VRPC type discrimination | Low | Medium | bigtable |

The **top 5 highest-ROI changes** are: parallel recovery downloads, parallel Sync uploads, batch pooling, single-allocation `EncodeCellKey`, and overlapping the WAL upload with local apply.
