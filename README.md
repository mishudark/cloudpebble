# CloudPebble

CloudPebble adds durable object storage persistence to [Pebble](https://github.com/cockroachdb/pebble), implementing an architecture similar to [turbopuffer](https://turbopuffer.com/docs/architecture).

A local Pebble instance serves as a read-optimized NVMe/SSD cache. All writes are durably committed to object storage (GCS) via a write-ahead log. Data is asynchronously indexed into SSTs and uploaded for cold reads and crash recovery.

```
                        ╔═══════════ cloudpebble ═══════════════════╗
╔════════════╗          ║                                           ║
║   client   ║───API──▶ ║  ┏━━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━┓  ║
║            ║          ║  ┃  Local Pebble   ┃───▶┃  GCS / S3 /  ┃  ║
╚════════════╝          ║  ┃ (Memory + SSD)  ┃    ┃  Azure Blob  ┃  ║
                        ║  ┗━━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━┛  ║
                        ╚══════════════════════════════════════════╝
```

## Architecture

### Write Path (Strong Consistency)

Every write creates an immutable WAL object in object storage. Once the WAL is durably committed, the write is acknowledged. The batch is also applied to a local Pebble instance for fast reads.

```
                    Set(key, value)
                         │
                         ▼
              ┌──────────────────────┐
              │  Encode Pebble batch │
              └──────────┬───────────┘
                         │
              ┌──────────▼───────────┐
              │  Write to GCS WAL    │  ── durability barrier (~100ms)
              │  {ns}/wal/{seq}.wal  │
              └──────────┬───────────┘
                         │
              ┌──────────▼───────────┐
              │  Apply to local      │
              │  Pebble memtable     │  ── visible immediately (~µs)
              └──────────┬───────────┘
                         │
              ┌──────────▼───────────┐
              │  Return success      │
              └──────────────────────┘
```

When batching is enabled (`BatchWindow > 0`, default 1s), concurrent writes within the same window are coalesced into a single GCS WAL object, matching turbopuffer's 1 WAL entry per second per namespace model.

```
                            Time ──────────────────────────────▶

  Write A ──┐
  Write B ──┤─── Batch window (1s) ───┐
  Write C ──┘                         │
                                       ▼
                              ┌────────────────┐
                              │  Single WAL     │
                              │  object {seq}   │──▶ GCS
                              │  (A + B + C)    │
                              └────────────────┘
```

### Read Path

```
  Get(key)
       │
       ▼
  ┌─────────────┐    miss    ┌──────────────────┐
  │ Local Pebble │──────────▶│ Cold-miss recovery│
  │ (mem + SSTs) │           │ (download SSTs +  │
  │              │           │  replay WALs from │
  └──────┬───────┘           │  object storage)  │
         │ hit               └────────┬─────────┘
         ▼                             │
  ┌─────────────┐                      │
  │ Return value │                     ▼
  │   (~ms)      │            ┌───────────────┐
  └──────────────┘            │ Return value   │
                              │   (~400ms)     │
                              └───────────────┘
```

### Async Flush + Upload (Background Indexer)

A background goroutine periodically flushes memtables to local SSTs, uploads them to object storage, and garbage-collects old WAL entries.

```
  syncLoop (every 30s)
       │
       ▼
  ┌──────────────┐
  │  AsyncFlush() │      New writes continue into next memtable
  │  (mem → SST)  │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ Checkpoint()  │      Consistent snapshot of LSM state
  │ (MANIFEST +   │
  │  SSTs)        │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ Upload new    │      Incremental: only upload changed SSTs
  │ SSTs to GCS   │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ Write manifest│      {version, max_wal_seq, files with checksums}
  │ to GCS        │
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ GC old WALs   │      Delete WAL objects covered by checkpoint
  │ + orphans     │
  └──────────────┘
```

### Recovery (Cold Start / Node Restart)

```
  Open(namespace)
       │
       ▼
  ┌─────────────────┐
  │ Read manifest    │
  │ from GCS         │
  └────────┬────────┘
           │
    ┌──────▼──────┐
    │ Has          │
    │ manifest?    │
    └──┬──────┬───┘
       │ no   │ yes
       ▼      ▼
  ┌────────┐  ┌───────────────────┐
  │ Start  │  │ Download SSTs +    │
  │ fresh  │  │ MANIFEST from GCS  │
  └───┬────┘  │ Verify checksums   │
      │       └────────┬──────────┘
      │                │
      └───────┬────────┘
              ▼
  ┌───────────────────────┐
  │ Open local Pebble     │
  └───────────┬───────────┘
              │
  ┌───────────▼───────────┐
  │ Consistency mode?     │
  └──┬────────────────┬───┘
     │ Strong         │ Eventual
     ▼                ▼
  ┌──────────────┐  ┌──────────────┐
  │ Replay all    │  │ Skip WAL      │
  │ unflushed WALs│  │ replay. Serve │
  │ from GCS      │  │ from checkpoint│
  └──────┬───────┘  │ Self-heal via  │
         │          │ background loop│
         ▼          └───────────────┘
  ┌──────────────┐
  │ Start serving │
  └──────────────┘
```

### GCS Object Layout

```
{namespace}/
├── manifest                        # Current manifest: {version, max_wal_seq, files[]}
├── manifests/
│   ├── 000001.json                 # Version history (last 10 kept)
│   └── 000002.json
├── data/
│   ├── MANIFEST-000003             # Pebble MANIFEST
│   ├── 000004.sst                  # SST files
│   ├── 000005.sst
│   ├── OPTIONS-000002              # Pebble options
│   └── marker.*                    # Pebble version markers
└── wal/
    ├── 00000000000000000001.wal    # Immutable WAL objects
    └── 00000000000000000002.wal    # (zero-padded 20-digit seq num)
```

## Consistency Models

| Mode | Open latency | Startup WAL replay | Staleness window | Self-healing |
|------|-------------|-------------------|------------------|--------------|
| `ConsistencyStrong` | Higher | Yes — replay all WALs | None (current) | Immediate |
| `ConsistencyEventual` | Lower | No — skip WALs | Up to last checkpoint | Background `walReplayLoop` |

Eventual consistency converges to strong over time as the background WAL replay loop catches up and the periodic Sync uploads new checkpoints.

## Bigtable API (gRPC)

CloudPebble includes a [Google Cloud Bigtable v2](https://cloud.google.com/bigtable/docs/reference/data-plane/rpc) compatible gRPC server that maps Bigtable's wide-column data model onto Pebble's key-value store. Each Bigtable table is backed by a separate CloudPebble namespace with its own Pebble DB + object storage durability.

The implementation follows the same key-encoding approach described in [Pinterest's Rockstorewidecolumn](https://medium.com/pinterest-engineering/building-pinterests-new-wide-column-database-using-rocksdb-f9b5f55d04e5).

### Data Model Mapping

```
Bigtable:  table / row_key / column_family / column_qualifier / timestamp → value
Pebble:    [row_len:2][row_key][00][family_len:1][family][00][qual_len:2][qualifier][00][inverted_ts:8] → value
```

Timestamps are inverted (`math.MaxInt64 - ts`) so the newest cells sort first under Pebble's ascending lexicographic order. Components are length-prefixed and separated by `0x00` sentinels, avoiding any escape-encoding overhead.

### Supported RPCs

| RPC | Status | Notes |
|-----|--------|-------|
| `ReadRows` | Done | Server-streaming CellChunk protocol, `RowSet`/`RowRange` support, `rows_limit` |
| `MutateRow` | Done | Atomic row-level mutations via Pebble `Batch` |
| `MutateRows` | Done | Batch mutations with per-entry `google.rpc.Status` |
| `CheckAndMutateRow` | Done | Atomic conditional mutate with predicate filter |
| `SampleRowKeys` | Done | Split-key sampling from SST file boundaries |
| `PingAndWarm` | Done | Health check + cache warming |
| `OpenTable` | Done | Session-based bidirectional streaming protocol |
| `ReadModifyWriteRow` | Stub | Returns `Unimplemented` |
| Change stream RPCs | Stub | Returns `Unimplemented` |
| SQL RPCs | Stub | Returns `Unimplemented` |

### RowFilter Support

| Filter | Status |
|--------|--------|
| `chain` | Done |
| `interleave` | Done (with duplicate suppression) |
| `condition` | Done |
| `pass_all_filter` / `block_all_filter` | Done |
| `row_key_regex_filter` | Done |
| `family_name_regex_filter` | Done |
| `column_qualifier_regex_filter` | Done |
| `column_range_filter` | Done |
| `timestamp_range_filter` | Done |
| `cells_per_row_offset_filter` | Done |
| `cells_per_row_limit_filter` | Done |
| `cells_per_column_limit_filter` | Done |
| `strip_value_transformer` | Done |
| `apply_label_transformer` | Done |
| `value_regex_filter` | Future |
| `value_range_filter` | Future |
| `row_sample_filter` | Future |
| `sink` | Future |

### Running the Bigtable Server

```bash
go run ./cmd/pebble-bigtable/ --addr :9000 --data-dir /tmp/btdb --object-dir /tmp/btobj
```

### Using with a Bigtable Client

```go
import (
    "github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
    "google.golang.org/grpc"
)

conn, _ := grpc.Dial("localhost:9000", grpc.WithInsecure())
client := bigtablepb.NewBigtableClient(conn)

// Direct RPCs
resp, _ := client.MutateRow(ctx, &bigtablepb.MutateRowRequest{
    TableName: "my-table",
    RowKey:    []byte("user123"),
    Mutations: []*bigtablepb.Mutation{
        {Mutation: &bigtablepb.Mutation_SetCell_{
            SetCell: &bigtablepb.Mutation_SetCell{
                FamilyName:      "profile",
                ColumnQualifier: []byte("name"),
                TimestampMicros: -1,
                Value:           []byte("Alice"),
            },
        }},
    },
})
```

### Architecture

```
Bigtable gRPC client
       │
       ▼
┌──────────────────────────────────────┐
│  pkg/bigtable/Server                  │
│  ├─ MutateRow → pebble.Batch.Apply    │
│  ├─ ReadRows  → pebble.Iterator       │
│  ├─ OpenTable → bidi session stream   │
│  └─ RowFilter → filter evaluator tree │
└──────────────┬───────────────────────┘
               │
    ┌──────────▼──────────┐
    │  pkg/engine/Engine   │  per-table namespace
    │  ├─ Pebble (cache)   │
    │  └─ GCS/local (WAL)  │
    └─────────────────────┘
```

## Usage (Embedded Engine)

```go
package main

import (
    "context"
    "log"

    "cloud.google.com/go/storage"
    "github.com/mishudark/cloudpebble/pkg/engine"
    "github.com/mishudark/cloudpebble/pkg/objstore/gcs"
)

func main() {
    store, _ := gcs.New("my-bucket", "cloudpebble/")

    e, err := engine.Open(engine.Options{
        Dir:       "/nvme/cloudpebble",
        Store:     store,
        Namespace: "my-namespace",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer e.Close()

    // Write — durably committed to GCS before returning.
    e.Set(context.Background(), []byte("hello"), []byte("world"))

    // Read — served from local Pebble cache.
    val, err := e.Get([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello = %s\n", val)

    // Access the underlying Pebble DB for advanced operations.
    iter, _ := e.DB().NewIter(nil)
    for iter.First(); iter.Valid(); iter.Next() {
        fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
    }
    iter.Close()

    // Metrics.
    snap := e.Metrics().Snapshot()
    fmt.Printf("Sets: %d, Gets: %d, Hits: %d, Misses: %d\n",
        snap.Sets, snap.Gets, snap.GetHits, snap.GetMisses)
}
```

### Using the Local Store (for development)

```go
import "github.com/mishudark/cloudpebble/pkg/objstore/local"

store, _ := local.New("/tmp/cloudpebble-objstore")
e, _ := engine.Open(engine.Options{
    Dir:       "/tmp/cloudpebble-cache",
    Store:     store,
    Namespace: "dev",
})
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `Dir` | `os.TempDir()` | Local directory for the Pebble cache |
| `Store` | **required** | Object storage backend (`gcs.Store`, `local.Store`, ...) |
| `Namespace` | `"default"` | Tenant/namespace prefix in object storage |
| `SyncInterval` | `30s` | Background checkpoint upload interval |
| `BatchWindow` | `1s` | WAL batching window (0 = disabled) |
| `ColdMissThreshold` | `3` | Consecutive misses before triggering recovery |
| `Consistency` | `Strong` | `Strong` or `Eventual` |
| `OrphanWALTTL` | `1h` | Delete orphan WAL objects older than this |
| `MaxLocalBytes` | `0` (unlimited) | Soft limit on local Pebble cache size |
| `PebbleOptions` | defaults | Passed through to `pebble.Open()` |

## Object Storage Interface

Backends implement the `Store` interface:

```go
type Store interface {
    io.Closer
    Put(ctx context.Context, path string, data []byte) error
    Get(ctx context.Context, path string) ([]byte, error)
    Delete(ctx context.Context, path string) error
    List(ctx context.Context, prefix string) ([]string, error)
    Exists(ctx context.Context, path string) (bool, error)
    Attrs(ctx context.Context, path string) (ObjectInfo, error)
}
```

| Backend | Package | Requires |
|---------|---------|----------|
| Google Cloud Storage | `objstore/gcs` | GCP credentials |
| Local filesystem | `objstore/local` | None (dev/test) |

To add a new backend, implement the `Store` interface and pass it to `engine.Options.Store`.

## Project Structure

```
cloudpebble/
├── BIGTABLE_PLAN.md                   # Bigtable API implementation plan
├── DESIGN.md                         # Architecture design document
├── PLAN.md                           # Implementation plan (shortcuts → production)
├── cmd/
│   ├── cloudpebble/main.go           # Demo CLI
│   ├── pebble-bigtable/main.go       # Bigtable gRPC server
│   ├── test-recovery/main.go         # Crash recovery integration test
│   ├── test-incremental/main.go      # Incremental upload test
│   ├── test-eventual/main.go         # Eventual consistency test
│   └── test-coldmiss/main.go         # Cold miss recovery test
├── pkg/
│   ├── bigtable/
│   │   ├── server.go                 # gRPC server + table registry
│   │   ├── session.go                # OpenTable bidi stream handler
│   │   ├── encoding.go               # Bigtable → Pebble key encoding
│   │   ├── mutate.go                 # MutateRow / MutateRows / CheckAndMutateRow
│   │   ├── readrows.go               # ReadRows: CellChunk streaming
│   │   ├── filter.go                 # RowFilter evaluation engine
│   │   ├── samplekeys.go             # SampleRowKeys
│   │   ├── types.go                  # RowProcessor callback type
│   │   └── bigtablepb/               # Generated proto Go code
│   ├── objstore/
│   │   ├── store.go                  # Store interface + ObjectInfo
│   │   ├── gcs/gcs.go                # Google Cloud Storage backend
│   │   ├── local/local.go            # Local filesystem backend
│   │   └── testutil/testutil.go      # Contract test harness
│   ├── walcloud/
│   │   └── manager.go                # WAL manager (write/read/list/GC/batch)
│   └── engine/
│       ├── engine.go                 # Core engine (Open/Set/Get/Sync/Close)
│       ├── metrics.go                # Metrics counters and latency histograms
│       └── engine_test.go            # Unit tests
└── go.mod / go.work / go.sum
```

## Testing

```bash
# Run all unit tests (32 tests, no credentials needed)
go test ./pkg/...

# Run integration tests (require local filesystem only)
go run ./cmd/test-recovery/ step1 && go run ./cmd/test-recovery/ step2
go run ./cmd/test-incremental/
go run ./cmd/test-eventual/ step1 && go run ./cmd/test-eventual/ step2
go run ./cmd/test-coldmiss/
```

Unit tests use `objstore/local` (no network, no credentials). The GCS backend has a contract test harness (`testutil.RunContractTests`) that all backends must pass.

## Benchmarks

| Operation | Batching disabled | Batching enabled (1s) |
|-----------|------------------|----------------------|
| Write latency | ~100ms (one GCS round-trip) | ~1ms (apply to memtable, GCS async) |
| Write throughput | ~10 writes/sec | ~10,000 writes/sec (amortized) |
| Warm read | ~ms | ~ms |
| Cold read | ~400ms (download + replay) | ~400ms |
| Cold start recovery | ~seconds | ~seconds (or ~ms with Eventual) |
