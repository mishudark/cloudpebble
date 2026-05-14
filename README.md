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

## Usage

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
├── DESIGN.md                         # Architecture design document
├── PLAN.md                           # Implementation plan (shortcuts → production)
├── cmd/
│   ├── cloudpebble/main.go           # Demo CLI
│   ├── test-recovery/main.go         # Crash recovery integration test
│   ├── test-incremental/main.go      # Incremental upload test
│   ├── test-eventual/main.go         # Eventual consistency test
│   └── test-coldmiss/main.go         # Cold miss recovery test
├── pkg/
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
