# CloudPebble Design

CloudPebble adds durable object storage persistence to [Pebble](https://github.com/cockroachdb/pebble),
enabling an architecture similar to [turbopuffer](https://turbopuffer.com/docs/architecture).

## Architecture

```
                        ╔═ cloudpebble ═══════════════════════════╗
╔════════════╗          ║                                         ║
║   client   ║───API──▶ ║  ┌──────────────┐     ┌─────────────┐  ║
║            ║          ║  │ Local Pebble  │────▶│ GCS (WAL +  │  ║
╚════════════╝          ║  │ (no local WAL)│     │  SSTs)      │  ║
                        ║  └──────────────┘     └─────────────┘  ║
                        ╚════════════════════════════════════════╝
```

### Key Design Decisions

- **WAL in object storage only.** Local Pebble runs with `DisableWAL: true`.
  GCS WAL objects are the sole durability layer. Write acknowledgement
  happens only after the WAL object is committed to object storage.

- **Local Pebble is a read-optimized cache.** After writes are durably
  committed to GCS, they are applied to a local Pebble instance for fast
  reads. Memtables are flushed to local SSTs, which are asynchronously
  uploaded to GCS.

- **Per-namespace isolation.** Each namespace has a separate GCS prefix
  and a separate local Pebble instance.

- **Pluggable object storage.** The `objstore.Store` interface abstracts
  the object storage backend. Google Cloud Storage is the primary
  implementation; additional backends (S3, Azure) can be added later.

## Write Path (Strong Consistency)

```
Set(k,v)
  → encode batch → Put GCS WAL object {ns}/wal/{00001}.wal
  → PWL (post-write log): record WAL seq, WAL path, batch payload
  → Apply to local Pebble memtable
  → Return success

                   User Write            
                     ┌─────┐             
                     │█████│             
  WAL                │█████│             
 gcs://{ns}/wal      └──┬──┘             
                        │                
 gcs://{ns}/wal/00001.wal                
 gcs://{ns}/wal/00002.wal                
 gcs://{ns}/wal/00003.wal                
```

## Read Path

```
Get(k)
  → local Pebble (memtable + SSTs) → return value     (warm, ~ms)
  → miss → download SST from GCS → retry              (cold, ~400ms)
```

## Async Flush + Upload

A background goroutine (like turbopuffer's indexer):
1. Detects new local SSTs from Pebble flushes
2. Uploads SSTs to GCS: `{ns}/sst/{file}.sst`
3. Updates manifest: `{ns}/manifest` — records which WAL seqs are covered
4. GCs old WAL objects from GCS

## Recovery (Cold Start / Node Restart)

```
Open(namespace):
  1. List GCS {ns}/sst/* → download all SSTs to local dir
  2. Read GCS {ns}/manifest → determine last flushed seq
  3. List GCS {ns}/wal/* → find WALs newer than last flush
  4. Open local Pebble with downloaded SSTs
  5. Replay unflushed WALs into Pebble
  6. Start serving
```

## GCS Object Layout

```
{namespace}/
├── manifest                    # Manifest: maps SST files to WAL seq ranges
├── wal/
│   ├── 0000000000000001.wal    # Immutable WAL objects (monotonic seq nums)
│   └── 0000000000000002.wal
└── sst/
    ├── 000001.sst              # SSTs uploaded from local flushes
    └── 000002.sst
```

## Interface: objstore.Store

```go
type Store interface {
    io.Closer
    Put(ctx context.Context, path string, data []byte) error
    Get(ctx context.Context, path string) ([]byte, error)
    Delete(ctx context.Context, path string) error
    List(ctx context.Context, prefix string) ([]string, error)
    Exists(ctx context.Context, path string) (bool, error)
}
```

## WAL Format

Each WAL object is a Pebble batch encoded via `batchrepr`. The WAL
object path encodes the sequence number for ordering:
`{namespace}/wal/{20-digit-seq}.wal`

## Multi-Tenancy

Namespaces provide multi-tenant isolation:
- Each namespace has its own GCS prefix
- Each namespace has its own local Pebble instance
- Namespace directory: `{baseDir}/{namespace}/`
