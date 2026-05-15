# Missing: Bigtable v2 API Implementation Gaps

This file tracks what remains unimplemented compared to the full
[Google Cloud Bigtable v2 proto definitions](https://github.com/googleapis/googleapis/tree/master/google/bigtable/v2).

---

## RPCs Not Implemented (return `Unimplemented`)

| RPC | What it does |
|-----|-------------|
| `GenerateInitialChangeStreamPartitions` | Apache Beam integration: returns partition map for change streams. |
| `ReadChangeStream` | Real-time change stream. Requires WAL/commit-log tracking. |
| `PrepareQuery` | GoogleSQL query preparation (parse + plan). |
| `ExecuteQuery` | GoogleSQL query execution with streaming `PartialResultSet`. |

---

## Session Protocol Gaps

| Gap | Details |
|-----|---------|
| `OpenAuthorizedView` | Stub (returns `Unimplemented`). |
| `OpenMaterializedView` | Stub (returns `Unimplemented`). |
| ~~`GetClientConfiguration`~~ | ~~Returns empty config. Should return `FeatureFlags` with supported capabilities.~~ Returns `SessionConfiguration` (session_load=1.0) and `stop_polling=true`. |
| Routing (`app_profile_id`) | Ignored on all requests. In production, app_profile_id routes to specific clusters. |
| Session lifecycle | `Heartbeat`, `GoAway`, `SessionRefreshConfig` messages not handled. |
| vRPC metadata routing | `VirtualRpcRequest.Metadata` ignored — RPC type detection via trial unmarshal instead. |

---

## Mutation Types Missing

| Mutation | Notes |
|----------|-------|
| `add_to_cell` | Requires `Aggregate` family type (`sum` aggregator). |
| `merge_to_cell` | Requires `Aggregate` family type (various aggregators). |

---

## RowFilter Types Missing (1 of 20)

| Filter | Notes |
|--------|-------|
| `sink` | Advanced output bypass. Copies matched cells to output while continuing evaluation of the parent filter. Used for filter-side-effect patterns. |

✅ **Implemented:**
- `value_regex_filter` — regex match on cell value bytes
- `value_range_filter` — lexicographic value range `[start, end)`
- `value_bitmask_filter` — per-byte `(value & mask) == mask`
- `row_sample_filter` — probabilistic row sampling at given rate

---

## Protocol / Semantics Gaps

### ReadRows

| Gap | Details |
|-----|---------|
| `request_stats_view` | `ReadRowsRequest.request_stats_view` ignored. `RequestStats` not populated in responses. |

✅ **Implemented:**
- `ReadRowsRequest.reversed` scans — uses Pebble `Last()` + `Prev()` with same bounds
- `ReadRowsResponse.last_scanned_row_key` — populated on each flushed response and final message
- CellChunk value chunking — values >64KB split across chunks with `value_size` hints
- CellChunk `reset_row` — `resetRowChunk()` helper and `rowTerminal()` checker added

### MutateRows

| Gap | Details |
|-----|---------|
| Idempotency | `idempotency` token (`Idempotency{token, start_time}`) ignored. Required for correctness with `Aggregate` families. |

✅ **Implemented:**
- `RateLimitInfo` (1s period, factor 1.0) returned in all `MutateRowsResponse` paths

### MutateRow

| Gap | Details |
|-----|---------|
| Idempotency | Same as MutateRows. |

### General

| Gap | Details |
|-----|---------|
| Response params | `ResponseParams` (zone_id, cluster_id) not set on responses. |
| Peer info | `PeerInfo` (transport_type, frontend_id) not handled. |
| Authorized views | `authorized_view_name` field ignored on all requests. |
| Materialized views | `materialized_view_name` field ignored on all requests. |
| Type system (types.proto) | `Type`, `Value`, `Aggregate`, `Struct`, `Array`, `Map` types generated but never used. All values are raw bytes. |
| Feature flags | `FeatureFlags` protos generated but not used in capability negotiation. |
| TTL per table | Not implemented. No time-to-live enforcement at the Bigtable layer. |
| Pagination markers | `ReadRows` doesn't support pagination with resumption markers (Pinterest-style `marker` parameter). |

---

## Missing Features by Effort

### Remaining Low Effort
- Populate `ResponseParams` with static zone/cluster

### Remaining Medium Effort
- Idempotency token support
- `sink` filter
- GoAway/Heartbeat in session protocol
- Pagination markers

### Remaining High Effort (major subsystems)
- `add_to_cell` / `merge_to_cell` (requires `Aggregate` family type support)
- Type system (strongly-typed values)
- Change stream RPCs (requires WAL tracking)
- SQL RPCs (requires SQL engine)
- Authorized views / Materialized views
- TTL per table (requires compaction filter + background GC)
- Robust vRPC metadata routing (session protocol dispatch)

### ✅ Completed
- `ReadRowsResponse.last_scanned_row_key` — populated in ReadRows
- `ReadRowsRequest.reversed` — reverse scans via Pebble Last/Prev
- `GetClientConfiguration` — returns session config with stop_polling
- `value_regex_filter` — regex on cell value bytes
- `value_range_filter` — lexicographic value range [start, end)
- `value_bitmask_filter` — per-byte bitmask comparison
- `row_sample_filter` — probabilistic row sampling
- CellChunk value chunking — large values split at 64KB boundary
- CellChunk `reset_row` — sentinel helper and terminal checker
- `RateLimitInfo` — returned in MutateRowsResponse with 1s period, factor 1.0
