# Bigtable API on CloudPebble — Implementation Plan

This document describes how to implement the Google Cloud Bigtable v2 data-plane
API on top of CloudPebble's existing engine (Pebble + object storage durability).

## Architecture

```
Client (Bigtable gRPC)
    │
    ▼
┌──────────────────────────────────────────────┐
│  bigtable.Server (gRPC)                       │
│  ├─ OpenTable session (bidi stream)           │
│  │   ├─ ReadRows → CellChunk stream           │
│  │   ├─ MutateRow → Pebble Batch + Apply      │
│  │   ├─ MutateRows → Batch per-entry status   │
│  │   ├─ CheckAndMutateRow → IndexedBatch      │
│  │   ├─ SampleRowKeys → SST boundary sampling │
│  │   └─ PingAndWarm → touch DB                │
│  └─ GetClientConfiguration                   │
│                                                │
│  Per-table: maps to cloudpebble engine.Engine  │
│  Per cell:   encoded as length-prefixed key    │
└──────────────┬───────────────────────────────┘
               │
    ┌──────────▼──────────┐
    │  engine.Engine       │  (existing)
    │  namespace="table"   │
    │  ├─ Set/Get/Apply    │
    │  ├─ Pebble (cache)   │
    │  └─ GCS/local (WAL)  │
    └─────────────────────┘
```

## Directory Layout

```
cloudpebble/
├── pkg/
│   ├── bigtable/                    # NEW PACKAGE
│   │   ├── server.go               # BigtableServer, table registry, session lifecycle
│   │   ├── session.go              # OpenTable bidi-stream handler, request routing
│   │   ├── encoding.go             # Length-prefixed key encode/decode
│   │   ├── readrows.go             # ReadRows: Pebble iter → CellChunk streaming
│   │   ├── mutate.go               # MutateRow, MutateRows, CheckAndMutateRow
│   │   ├── samplekeys.go           # SampleRowKeys from SST boundaries
│   │   ├── filter.go               # RowFilter evaluation (core filters)
│   │   ├── ttl.go                  # TTL enforcement (read-time + compaction filter)
│   │   ├── pagination.go           # Paginated responses (marker-based)
│   │   └── bigtablepb/             # Generated proto Go code
│   ├── engine/                      # (existing — unchanged)
│   ├── objstore/                    # (existing — unchanged)
│   └── walcloud/                    # (existing — unchanged)
├── cmd/
│   └── pebble-bigtable/            # NEW: server binary
│       └── main.go
└── go.mod / go.work
```

## Key Encoding

Each Bigtable cell maps to one Pebble key-value pair using length-prefixed
components. No escape encoding needed — each component's length is stored
before the data so prefixes compare correctly.

```
[2B row_key_len][row_key][1B family_len][family][2B qual_len][qualifier][8B inverted_ts]
                                                        Value: [cell_value]
```

| Component | Encoding | Notes |
|-----------|----------|-------|
| `row_key_len` | 2 bytes uint16 big-endian | Bigtable max 4KiB |
| `row_key` | N bytes | Arbitrary bytes |
| `family_len` | 1 byte uint8 | Family names: 1-64 chars, `[-_.a-zA-Z0-9]+` |
| `family` | N bytes | ASCII only, no 0x00 |
| `qual_len` | 2 bytes uint16 big-endian | Bigtable max 16KiB |
| `qualifier` | N bytes | Arbitrary bytes |
| `inverted_ts` | 8 bytes uint64 big-endian | `math.MaxInt64 - timestamp_micros` |

**Why inverted_ts:** Pebble sorts ascending. Subtracting the timestamp from
MaxInt64 makes the newest (largest timestamp) cell have the smallest inverted
value and thus sort first within a row+family+qualifier prefix.

**Prefix scan examples:**

| Operation | Pebble iterator bounds |
|-----------|----------------------|
| All cells for row R | `[len(R):2][R]` → `[len(R):2][R][0xFF]...` |
| All cells for row R, family F | `[len(R):2][R][len(F):1][F]` → `[len(R):2][R][len(F):1][F][0xFF]...` |
| All cells for row R, family F, qual Q | `[len(R):2][R][len(F):1][F][len(Q):2][Q]` → next key beyond |

## Phase 1 RPCs (with Session Protocol)

The Bigtable session protocol (session.proto) wraps data-plane RPCs inside a
bidirectional stream. The client opens a session per table, then sends
SessionRequest messages wrapping the actual operation.

**RPC Service: Bigtable**

| RPC | Type | Description |
|-----|------|-------------|
| `GetClientConfiguration` | unary | Return server capabilities |
| `OpenTable` | bidi stream | Session lifecycle + request routing |

**Within a session (SessionRequest oneof):**

| Operation | Handler | Response |
|-----------|---------|----------|
| `read_rows` | In-session | Streams ReadRowsResponse chunks |
| `mutate_row` | In-session | Empty MutateRowResponse |
| `mutate_rows` | In-session | MutateRowsResponse with per-entry status |
| `check_and_mutate_row` | In-session | CheckAndMutateRowResponse with predicate_matched |
| `sample_row_keys` | In-session | Streams SampleRowKeysResponse entries |
| `ping_and_warm` | In-session | Empty PingAndWarmResponse |

## How Each RPC Maps to Engine/Pebble

| RPC | Engine/Pebble operation |
|-----|------------------------|
| `GetClientConfiguration` | Return static capabilities |
| `OpenTable` | `engine.Open(dir, store, namespace=tableName)` → holds Engine for session |
| `ReadRows` | `db.NewIter(bounds)` → iterate → apply filter → emit CellChunks |
| `MutateRow` | Build `pebble.Batch` from mutations → `engine.Apply(ctx, batch)` |
| `MutateRows` | Build one Batch with all entries → Apply → map status per entry |
| `CheckAndMutateRow` | `db.NewIndexedBatch()` with mutations → iterate predicate → Apply |
| `SampleRowKeys` | `db.SSTables()` → compute split keys → stream |
| `PingAndWarm` | `db.Metrics()` → return empty response |

## Mutation Mapping

| Bigtable Mutation | Pebble Operation |
|-------------------|-----------------|
| `set_cell` | `batch.Set(encodedKey, value, nil)` |
| `delete_from_column` | `batch.DeleteRange(colStart, colEnd)` |
| `delete_from_family` | `batch.DeleteRange(familyStart, familyEnd)` |
| `delete_from_row` | `batch.DeleteRange(rowStart, rowEnd)` |
| `add_to_cell` / `merge_to_cell` | Not in Phase 1 (requires `Aggregate` family support) |

## RowFilter Engine

Phase 1 implements these filter types:

| Filter | Implementation |
|--------|---------------|
| `chain` | Sequential pipeline: output of filter N = input of filter N+1 |
| `interleave` | Parallel: each sub-filter reads from source, results merged (duplicate suppression) |
| `condition` | If/else: apply predicate → if match, use true_filter else false_filter |
| `pass_all_filter` | Passthrough |
| `block_all_filter` | Drops all cells |
| `row_key_regex_filter` | Drops rows whose key doesn't match |
| `family_name_regex_filter` | Drops families whose name doesn't match |
| `column_qualifier_regex_filter` | Drops qualifiers not matching |
| `column_range_filter` | Drops cells outside the ColumnRange |
| `timestamp_range_filter` | Drops cells outside the TimestampRange |
| `cells_per_row_offset_filter` | Skips first N cells per row |
| `cells_per_row_limit_filter` | Keeps at most N cells per row |
| `cells_per_column_limit_filter` | Keeps at most N cells per column (family+qualifier) |
| `strip_value_transformer` | Replaces cell value with empty |
| `apply_label_transformer` | Tags matched cells with a label string |

Deferred to Phase 2:

| Filter | Reason |
|--------|--------|
| `value_regex_filter` | Simple but lower priority |
| `value_range_filter` | Requires ValueRange parsing |
| `value_bitmask_filter` | Niche |
| `row_sample_filter` | Statistical, non-deterministic |
| `sink` | Advanced output bypass |

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage per table | Each Bigtable table = one cloudpebble namespace | Isolation, independent Pebble DB, independent GCS prefix |
| Table lifecycle | Tables pre-created as directories. `OpenTable` opens/attaches Engine. | Simpler than dynamic table creation |
| Mutations atomicity | Pebble `Batch.Apply` — atomic at row level. For `MutateRows`, one Batch covers all rows. | Stronger guarantee than Bigtable requires (per-row atomicity) |
| WAL durability | Engine already handles GCS WAL + Pebble apply. Bigtable layer just calls `engine.Apply()`. | Reuses existing durability infrastructure |
| CellChunk streaming | Buffered: collect cells from Pebble iter, emit in response messages when 100 cells or 1MB accumulated, or when commit_row sentinel reached. | Matches Bigtable's streaming protocol |
| Timestamp handling | `timestamp_micros = -1` → server assigns `time.Now().UnixMicro()`. TTL via read-time Expired check. | Standard Bigtable server behavior |
| Table naming | Strip `projects/{p}/instances/{i}/tables/{t}` → use `{t}` as namespace. | Simple; full path validation can be added later |

## Dependencies to Add

```
google.golang.org/grpc              # gRPC server + client
google.golang.org/protobuf          # Protobuf runtime
google.golang.org/genproto/googleapis/rpc/status  # For MutateRowsResponse status
```

## Implementation Order

| Step | Files | Effort |
|------|-------|--------|
| 1. Proto generation + go.mod | `bigtablepb/`, `go.mod` | Setup |
| 2. Key encoding | `encoding.go` | Small |
| 3. Server skeleton + table registry | `server.go` | Medium |
| 4. Session protocol (OpenTable bidi) | `session.go` | Medium |
| 5. MutateRow + MutateRows | `mutate.go` | Medium |
| 6. ReadRows (CellChunk streaming) | `readrows.go` | Large |
| 7. RowFilter engine | `filter.go` | Large |
| 8. CheckAndMutateRow | `mutate.go` | Small |
| 9. SampleRowKeys + PingAndWarm | `samplekeys.go` | Small |
| 10. Server binary | `cmd/pebble-bigtable/main.go` | Small |
| 11. Tests | `*_test.go` | Medium |

## Testing Strategy

Unit tests use `objstore/local` for the storage backend. Integration tests
spin up a gRPC server, connect with a Bigtable client, and exercise each RPC.

Tests cover:
- Encoding round-trip (encode → decode → verify)
- Key prefix scan correctness (row, family, qualifier boundaries)
- ReadRows: RowSet (keys + ranges), CellChunk reconstruction, filter application
- MutateRow: all mutation types, timestamp ordering
- MutateRows: batch with per-entry status, partial failures
- CheckAndMutateRow: predicate match/mismatch
- RowFilter: each filter type, chain/interleave/condition combinators

## Reference

- Pinterest Rockstorewidecolumn blog post: similar RocksDB → wide column mapping
- Bigtable proto definitions: `/home/thinkpad/src/github.com/google/googleapis/google/bigtable/v2/`
- CloudPebble engine: `pkg/engine/engine.go`
