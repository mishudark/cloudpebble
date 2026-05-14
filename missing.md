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
| `GetClientConfiguration` | Returns empty config. Should return `FeatureFlags` with supported capabilities. |
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

## RowFilter Types Missing (5 of 20)

| Filter | Notes |
|--------|-------|
| `sink` | Advanced output bypass. Copies matched cells to output while continuing evaluation of the parent filter. Used for filter-side-effect patterns. |
| `row_sample_filter` | Probabilistic row sampling (float `[0,1]`). Non-deterministic. |
| `value_regex_filter` | Regex match on cell value bytes. |
| `value_range_filter` | Lexicographic value range `[start, end)`. Requires `ValueRange` with `start_value_closed/open` and `end_value_closed/open` oneofs. |
| `value_bitmask_filter` | Bitmask comparison `(value & mask) == mask`. |

---

## Protocol / Semantics Gaps

### ReadRows

| Gap | Details |
|-----|---------|
| Reverse scans | `ReadRowsRequest.reversed` not supported. Requires reverse iteration (`iter.Last()` + `iter.Prev()`). |
| Resumable reads | `ReadRowsResponse.last_scanned_row_key` not populated. Clients use this to resume interrupted scans. |
| CellChunk value chunking | Values emitted in a single chunk. `value_size` hint and multi-chunk value assembly not implemented. |
| CellChunk `reset_row` | Error recovery sentinel not handled. Should cause the row being built to be discarded. |
| `request_stats_view` | `ReadRowsRequest.request_stats_view` ignored. `RequestStats` not populated in responses. |

### MutateRows

| Gap | Details |
|-----|---------|
| Rate limiting | `RateLimitInfo` (period + factor) not returned in `MutateRowsResponse`. |
| Idempotency | `idempotency` token (`Idempotency{token, start_time}`) ignored. Required for correctness with `Aggregate` families. |

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

### Low Effort (protocol completeness)
- Populate `ReadRowsResponse.last_scanned_row_key`
- Populate `ResponseParams` with static zone/cluster
- `ReadRowsRequest.reversed` scans
- Return `FeatureFlags` in `GetClientConfiguration`
- `value_regex_filter`
- `value_range_filter`
- `value_bitmask_filter`
- `row_sample_filter`

### Medium Effort
- CellChunk value chunking (split large values across chunks)
- CellChunk `reset_row` handling
- `RateLimitInfo` in MutateRowsResponse
- Idempotency token support
- `sink` filter
- GoAway/Heartbeat in session protocol
- Pagination markers

### High Effort (major subsystems)
- `add_to_cell` / `merge_to_cell` (requires `Aggregate` family type support)
- Type system (strongly-typed values)
- Change stream RPCs (requires WAL tracking)
- SQL RPCs (requires SQL engine)
- Authorized views / Materialized views
- TTL per table (requires compaction filter + background GC)
- Robust vRPC metadata routing (session protocol dispatch)
