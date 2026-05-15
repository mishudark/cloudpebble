# Using the Official Go Bigtable Client Library

CloudPebble includes a [Google Cloud Bigtable v2](https://cloud.google.com/bigtable/docs/reference/data-plane/rpc)-compatible gRPC server. This means you can use the official `cloud.google.com/go/bigtable` Go client library (or any Bigtable v2 SDK) to read and write data.

## Running the Server

```bash
go run ./cmd/pebble-bigtable/ --addr :9000 --data-dir /tmp/btdb --object-dir /tmp/btobj
```

The server auto-creates tables on first access — there is no `CreateTable` RPC. Use `PingAndWarm` to pre-open a table.

## Using the Official Go Client

### 1. Add the dependency

```bash
go get cloud.google.com/go/bigtable
```

### 2. Connect using a custom gRPC dial option

The Bigtable client expects to talk to the real Bigtable service. To point it at CloudPebble, inject a custom `grpc.NewClient` via `bigtable.ClientConfig`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.NewClient("localhost:9000",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// The project/instance values are ignored by CloudPebble but are
	// required by the client API. Use any non-empty strings.
	adminClient, err := bigtable.NewAdminClient(ctx, "project", "instance",
		option.WithGRPCConn(conn),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer adminClient.Close()

	// CloudPebble auto-creates tables — just list to verify connectivity.
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Tables: %v\n", tables)
}
```

> **Note**: CloudPebble skips project/instance routing. Every table exists under a flat namespace (`bigtable/<table_name>`). Use `grpc.NewClient` (not the deprecated `grpc.Dial`) for connecting.

### 3. Read and write data

```go
client, err := bigtable.NewClient(ctx, "project", "instance",
    option.WithGRPCConn(conn),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

tbl := client.Open("my-table")

// Write a row.
mut := bigtable.NewMutation()
mut.Set("profile", "name", bigtable.Now(), []byte("Alice"))
mut.Set("profile", "email", bigtable.Now(), []byte("alice@example.com"))
if err := tbl.Apply(ctx, "user123", mut); err != nil {
    log.Fatal(err)
}

// Read a specific row.
row, err := tbl.ReadRow(ctx, "user123")
if err != nil {
    log.Fatal(err)
}
for famName, items := range row {
	for _, item := range items {
		fmt.Printf("%s:%s @ %d = %q\n",
			famName, item.Column, item.Timestamp, item.Value)
	}
}

// Scan rows with a filter.
filter := bigtable.PassAllFilter()
err = tbl.ReadRows(ctx, bigtable.RowRange{},
    func(row bigtable.Row) bool {
        fmt.Printf("Row: %s\n", row.Key())
        return true
    },
    bigtable.RowFilter(filter),
)
if err != nil {
    log.Fatal(err)
}
```

### 4. Atomic conditional mutations

```go
// CheckAndMutateRow: apply the true mutation only if the row
// already contains a cell matching the predicate filter.
trueMut := bigtable.NewMutation()
trueMut.Set("cf1", "updated", bigtable.Now(), []byte("yes"))

falseMut := bigtable.NewMutation()
falseMut.Set("cf1", "fallback", bigtable.Now(), []byte("no-match"))

condMut := bigtable.NewCondMutation(
	bigtable.ChainFilters(
		bigtable.FamilyFilter("status"),
		bigtable.ColumnFilter("active"),
	),
	trueMut,
	falseMut,
)

var matched bool
err := tbl.Apply(ctx, "user123", condMut, bigtable.GetCondMutationResult(&matched))
if err != nil {
	log.Fatal(err)
}
fmt.Printf("Predicate matched: %v\n", matched)
```

### 5. ReadModifyWriteRow (Append/Increment)

```go
// Increment a counter (stored as 8-byte big-endian int64).
mut := bigtable.NewReadModifyWrite()
mut.Increment("stats", "visits", 1)
row, err := tbl.ApplyReadModifyWrite(ctx, "page:/about", mut)
if err != nil {
    log.Fatal(err)
}

	// Append to a value.
	mut = bigtable.NewReadModifyWrite()
	mut.AppendValue("log", "events", []byte(";new-event"))
	row, err = tbl.ApplyReadModifyWrite(ctx, "user123", mut)
```

### 6. Bulk mutations

```go
keys := []string{"row1", "row2", "row3"}
mut := bigtable.NewMutation()
mut.Set("cf", "q", bigtable.Now(), []byte("bulk"))

errs, err := tbl.ApplyBulk(ctx, keys, []*bigtable.Mutation{mut, mut, mut})
if err != nil {
    log.Fatal(err)
}
for _, e := range errs {
    if e != nil {
        log.Printf("entry error: %v", e)
    }
}
```

## Testing with the Official Client

The `cmd/test-bigtable-client` binary runs a full integration suite against a locally-started `pebble-bigtable` server using the official `cloud.google.com/go/bigtable` client:

```bash
# Run all 33 tests
go run ./cmd/test-bigtable-client/

# Run tests + 500 random fuzz iterations
go run ./cmd/test-bigtable-client/ --fuzz 500
```

Test coverage includes:
- Mutations (SetCell, DeleteFromColumn/Family/Row, DeleteTimestampRange, ApplyBulk)
- Reads (ReadRow, full scans, RowList, PrefixRange, binary keys)
- Filters (all 16 filter types including chain/interleave/condition)
- RowSet types (single key, range, prefix, row list)
- Reversed scans with strict decreasing row key order
- Large values split across multiple CellChunks (up to 100 KB)
- Atomic operations (CheckAndMutateRow, ReadModifyWrite)
- SampleRowKeys and concurrent writes

## Filter Reference

CloudPebble implements the following RowFilter types. They work identically whether used via the official client or raw gRPC.

| Filter | Supported |
|--------|-----------|
| `chain` / `interleave` / `condition` | Yes |
| `pass_all_filter` / `block_all_filter` | Yes |
| `row_key_regex_filter` | Yes |
| `family_name_regex_filter` | Yes |
| `column_qualifier_regex_filter` | Yes |
| `column_range_filter` | Yes |
| `timestamp_range_filter` | Yes |
| `cells_per_row_offset_filter` | Yes |
| `cells_per_row_limit_filter` | Yes |
| `cells_per_column_limit_filter` | Yes |
| `strip_value_transformer` | Yes |
| `apply_label_transformer` | Yes |
| `value_regex_filter` | Yes |
| `value_range_filter` | Yes |
| `value_bitmask_filter` | Yes |
| `row_sample_filter` | Yes (crypto-random) |
| `sink` | No (treated as pass_all) |

## Unsupported RPCs

These Bigtable RPCs return `Unimplemented`:

- `GenerateInitialChangeStreamPartitions` / `ReadChangeStream`
- `PrepareQuery` / `ExecuteQuery`
- `OpenAuthorizedView` / `OpenMaterializedView`

## Configuration Reference

Server flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `:9000` | gRPC listen address |
| `--data-dir` | `/tmp/cloudpebble-bigtable` | Local Pebble data directory |
| `--object-dir` | `/tmp/cloudpebble-bigtable-obj` | Object storage directory |

Engine options available programmatically via `engine.Options` (for embedded use):

| Option | Default | Description |
|--------|---------|-------------|
| `Consistency` | `Strong` | `Strong` (replay WALs on open) or `Eventual` |
| `BatchWindow` | `200ms` | WAL batching window (negative = disabled) |
| `SyncInterval` | `30s` | Background checkpoint upload interval |
| `ColdMissThreshold` | `3` | Consecutive misses before self-heal |
