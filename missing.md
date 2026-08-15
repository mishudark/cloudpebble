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
| ~~Session lifecycle~~ | ~~`Heartbeat`, `GoAway`, `SessionRefreshConfig` messages not handled.~~ Not implementable with current protos: `SessionRequest` carries only OpenSession/CloseSession/VirtualRpc payloads; Heartbeat/GoAway are server→client only. `SessionRefreshConfig` still unhandled. |
| vRPC metadata routing | `VirtualRpcRequest.Metadata` ignored — RPC type detection via trial unmarshal instead. |

---

## Mutation Types

All mutation types are now implemented. See ✅ Completed below.

---

## RowFilter Types

All 20 RowFilter types are now implemented. See ✅ Completed below.

---

## Protocol / Semantics Gaps

### ReadRows

✅ **Implemented:**
- `ReadRowsRequest.reversed` scans — uses Pebble `Last()` + `Prev()` with same bounds
- `ReadRowsResponse.last_scanned_row_key` — populated on each flushed response and final message
- CellChunk value chunking — values >64KB split across chunks with `value_size` hints
- CellChunk `reset_row` — `resetRowChunk()` helper and `rowTerminal()` checker added
- `request_stats_view` — `REQUEST_STATS_FULL` returns `RequestStats` (ReadIterationStats + FrontendServerLatency) on the final response

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
| Response params | `ResponseParams` (zone_id, cluster_id) not set on responses. **Not implementable with current protos**: `ResponseParams` is not embedded in any generated response message. |
| Session lifecycle | `Heartbeat`, `GoAway` handling not implementable with current protos: `SessionRequest` carries only OpenSession/CloseSession/VirtualRpc payloads; Heartbeat/GoAway are server→client only. |
| Peer info | `PeerInfo` (transport_type, frontend_id) not handled. |
| Authorized views | `authorized_view_name` field ignored on all requests. |
| Materialized views | `materialized_view_name` field ignored on all requests. |
| Type system (types.proto) | `Type`, `Value`, `Aggregate`, `Struct`, `Array`, `Map` types generated but never used. All values are raw bytes. |
| Feature flags | `FeatureFlags` protos generated but not used in capability negotiation. |
| TTL per table | Not implemented. No time-to-live enforcement at the Bigtable layer. |
| Pagination markers | `ReadRows` doesn't support pagination with resumption markers (Pinterest-style `marker` parameter). |

---

## Missing Features by Effort

### Remaining Medium Effort
- Idempotency token support
- Pagination markers

### Remaining High Effort (major subsystems)
- Aggregate mutations beyond int64 sum (max/min/HLL aggregators, `Aggregate` family schema)
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
- `add_to_cell` / `merge_to_cell` — int64-sum aggregates via Pebble Merge operands (atomic, commutative; no read-modify-write race). Timestamps forced to 0 (timeless counters); non-8-byte existing cells rejected with FailedPrecondition. Only the sum aggregator is supported; max/min/HLL and `Aggregate` family schema are not.
- `sink` filter — per-cell bypass: cells reaching the sink in a chain are emitted regardless of the parent verdict
- `request_stats_view` — REQUEST_STATS_FULL returns rows/cells seen vs returned plus frontend latency on the final ReadRows response
