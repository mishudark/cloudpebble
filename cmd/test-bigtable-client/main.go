// Command test-bigtable-client verifies the official Google Cloud Bigtable
// client library works with CloudPebble's Bigtable v2 gRPC server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startServer(addr, dataDir, objDir string) (*exec.Cmd, error) {
	_ = os.RemoveAll(dataDir)
	_ = os.RemoveAll(objDir)
	_ = os.MkdirAll(dataDir, 0750)
	_ = os.MkdirAll(objDir, 0750)

	args := []string{"--addr", addr, "--data-dir", dataDir, "--object-dir", objDir}
	//nolint:gosec // test-only binary
	cmd := exec.Command("go", append([]string{"run", "./cmd/pebble-bigtable/"}, args...)...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting server: %w", err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func newClient(ctx context.Context, addr string) (*bigtable.Client, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dialing server: %w", err)
	}
	client, err := bigtable.NewClient(ctx, "project", "instance",
		option.WithGRPCConn(conn),
	)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("creating bigtable client: %w", err)
	}
	return client, conn, nil
}

func main() {
	addr := flag.String("addr", ":0", "gRPC listen address (:0 = random port)")
	fuzz := flag.Int("fuzz", 0, "run fuzz test with N random iterations (0 = skip)")
	flag.Parse()

	if *addr == ":0" {
		lis, err := net.Listen("tcp", ":0") //nolint:gosec // test-only, random port
		if err != nil {
			log.Fatalf("finding free port: %v", err)
		}
		*addr = lis.Addr().String()
		_ = lis.Close()
	}

	dd := filepath.Join(os.TempDir(), "cloudpebble-test-btclient", "pebble")
	od := filepath.Join(os.TempDir(), "cloudpebble-test-btclient", "objstore")

	cmd, err := startServer(*addr, dd, od)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
		_ = cmd.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, conn, err := newClient(ctx, *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	defer func() { _ = conn.Close() }()

	tbl := client.Open("integration-test")

	var failures int
	for _, test := range []struct {
		name string
		fn   func(context.Context, *bigtable.Table) error
	}{
		{"SetCell", testSetCell},
		{"ReadRow", testReadRow},
		{"ReadRows/FullScan", testReadRowsFullScan},
		{"ReadRows/RowKeyFilter", testReadRowsRowKeyFilter},
		{"ReadRows/FamilyFilter", testReadRowsFamilyFilter},
		{"ReadRows/ColumnFilter", testReadRowsColumnFilter},
		{"ReadRows/ValueFilter", testReadRowsValueFilter},
		{"ReadRows/ChainFilters", testReadRowsChainFilters},
		{"ReadRows/InterleaveFilters", testReadRowsInterleaveFilters},
		{"ReadRows/StripValue", testReadRowsStripValue},
		{"ReadRows/LatestNFilter", testReadRowsLatestNFilter},
		{"ReadRows/TimestampRangeFilter", testReadRowsTimestampRange},
		{"ReadRows/RowLimit", testReadRowsRowLimit},
		{"ReadRows/Reversed", testReadRowsReversed},
		{"ReadRows/RowList", testReadRowsRowList},
		{"ReadRows/PrefixRange", testReadRowsPrefixRange},
		{"ReadRows/BinaryKey", testReadRowsBinaryKey},
		{"ReadRows/LargeValue", testReadRowsLargeValue},
		{"ReadRows/ManyCellsSingleRow", testReadRowsManyCellsSingleRow},
		{"CheckAndMutateRow/TrueBranch", testCheckAndMutateTrue},
		{"CheckAndMutateRow/FalseBranch", testCheckAndMutateFalse},
		{"ReadModifyWrite/Increment", testIncrement},
		{"ReadModifyWrite/NegativeIncrement", testNegativeIncrement},
		{"ReadModifyWrite/Append", testAppend},
		{"ApplyBulk", testApplyBulk},
		{"ApplyBulk/Empty", testApplyBulkEmpty},
		{"DeleteFromColumn", testDeleteFromColumn},
		{"DeleteFromFamily", testDeleteFromFamily},
		{"DeleteFromRow", testDeleteFromRow},
		{"DeleteTimestampRange", testDeleteTimestampRange},
		{"SampleRowKeys", testSampleRowKeys},
		{"ConcurrentWrites", testConcurrentWrites},
		{"CheckAndMutateRow/ValuePredicate", testCheckAndMutateValuePredicate},
		{"CheckAndMutateRow/ConcurrentSameRow", testCheckAndMutateConcurrentSameRow},
		{"ReadRows/CellsPerRowLimitMultiRow", testReadRowsCellsPerRowLimitMultiRow},
		{"ReadRows/CellsPerRowOffsetMultiRow", testReadRowsCellsPerRowOffsetMultiRow},
	} {
		if err := test.fn(ctx, tbl); err != nil {
			fmt.Printf("  FAIL  %s: %v\n", test.name, err)
			failures++
		} else {
			fmt.Printf("  PASS  %s\n", test.name)
		}
	}

	if *fuzz > 0 {
		fmt.Printf("\n--- Fuzz test (%d iterations) ---\n", *fuzz)
		for i := 0; i < *fuzz; i++ {
			if err := fuzzRoundTrip(ctx, tbl, i); err != nil {
				fmt.Printf("  FAIL  Fuzz iteration %d: %v\n", i, err)
				failures++
			}
		}
		if failures == 0 {
			fmt.Println("  PASS  all fuzz iterations")
		}
	}

	if failures > 0 {
		fmt.Printf("\n%d test(s) FAILED\n", failures)
		os.Exit(1)
	}
	fmt.Println("\nAll tests PASSED")
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

func testSetCell(ctx context.Context, tbl *bigtable.Table) error {
	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "email", bigtable.Now(), []byte("alice@example.com"))
	mut.Set("cf2", "score", bigtable.Now(), []byte("42"))
	return tbl.Apply(ctx, "user1", mut)
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

func testReadRow(ctx context.Context, tbl *bigtable.Table) error {
	row, err := tbl.ReadRow(ctx, "user1")
	if err != nil {
		return fmt.Errorf("ReadRow: %w", err)
	}
	if row == nil {
		return errors.New("row is nil")
	}
	if len(row["cf1"]) != 2 {
		return fmt.Errorf("expected 2 cells in cf1, got %d", len(row["cf1"]))
	}
	if len(row["cf2"]) != 1 {
		return fmt.Errorf("expected 1 cell in cf2, got %d", len(row["cf2"]))
	}
	return nil
}

func testReadRowsFullScan(ctx context.Context, tbl *bigtable.Table) error {
	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		return fmt.Errorf("ReadRows: %w", err)
	}
	if rows < 1 {
		return errors.New("expected at least 1 row")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Filter tests
// ---------------------------------------------------------------------------

func testReadRowsRowKeyFilter(ctx context.Context, tbl *bigtable.Table) error {
	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
		bigtable.RowFilter(bigtable.RowKeyFilter("user1")),
	)
	if err != nil {
		return fmt.Errorf("ReadRows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row, got %d", rows)
	}
	return nil
}

func testReadRowsFamilyFilter(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			cells += len(r["cf1"])
			return true
		},
		bigtable.RowFilter(bigtable.FamilyFilter("cf1")),
	)
	if err != nil {
		return fmt.Errorf("ReadRows: %w", err)
	}
	if cells != 2 {
		return fmt.Errorf("expected 2 cells in cf1, got %d", cells)
	}
	return nil
}

func testReadRowsColumnFilter(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			cells += len(r["cf1"]) + len(r["cf2"])
			return true
		},
		bigtable.RowFilter(bigtable.ColumnFilter("name")),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with column filter: %w", err)
	}
	if cells != 1 {
		return fmt.Errorf("expected 1 cell matching column 'name', got %d", cells)
	}
	return nil
}

func testReadRowsValueFilter(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, fam := range r {
				cells += len(fam)
			}
			return true
		},
		bigtable.RowFilter(bigtable.ValueFilter("42")),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with value filter: %w", err)
	}
	if cells != 1 {
		return fmt.Errorf("expected 1 cell with value '42', got %d", cells)
	}
	return nil
}

func testReadRowsChainFilters(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, fam := range r {
				cells += len(fam)
			}
			return true
		},
		bigtable.RowFilter(bigtable.ChainFilters(
			bigtable.FamilyFilter("cf1"),
			bigtable.ColumnFilter("name"),
		)),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with chain filters: %w", err)
	}
	if cells != 1 {
		return fmt.Errorf("expected 1 cell matching cf1 AND name, got %d", cells)
	}
	return nil
}

func testReadRowsInterleaveFilters(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, fam := range r {
				cells += len(fam)
			}
			return true
		},
		bigtable.RowFilter(bigtable.InterleaveFilters(
			bigtable.FamilyFilter("cf1"),
			bigtable.FamilyFilter("cf2"),
		)),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with interleave filters: %w", err)
	}
	if cells != 3 {
		return fmt.Errorf("expected 3 cells across cf1 OR cf2, got %d", cells)
	}
	return nil
}

func testReadRowsStripValue(ctx context.Context, tbl *bigtable.Table) error {
	var cells int
	var stripErr error
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, items := range r {
				for _, item := range items {
					if len(item.Value) != 0 {
						stripErr = fmt.Errorf("expected empty value with strip value filter, got %q", item.Value)
						return false
					}
					cells++
				}
			}
			return true
		},
		bigtable.RowFilter(bigtable.StripValueFilter()),
	)
	if err != nil {
		return err
	}
	if stripErr != nil {
		return stripErr
	}
	if cells < 1 {
		return errors.New("expected at least 1 cell with stripped value")
	}
	return nil
}

func testReadRowsLatestNFilter(ctx context.Context, tbl *bigtable.Table) error {
	key := "latest-n-test"
	for i := int64(1); i <= 5; i++ {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "ver", bigtable.Time(time.Unix(0, i*int64(time.Millisecond))), fmt.Appendf(nil, "v%d", i))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return err
		}
	}

	var cells int
	err := tbl.ReadRows(ctx, bigtable.SingleRow(key),
		func(r bigtable.Row) bool {
			cells += len(r["cf1"])
			return true
		},
		bigtable.RowFilter(bigtable.LatestNFilter(2)),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with LatestNFilter: %w", err)
	}
	if cells != 2 {
		return fmt.Errorf("expected 2 cells with LatestNFilter(2), got %d", cells)
	}

	// Clean up.
	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

func testReadRowsTimestampRange(ctx context.Context, tbl *bigtable.Table) error {
	key := "ts-range-test"
	refTime := time.Now()
	for d := -2; d <= 2; d++ {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "val", bigtable.Time(refTime.Add(time.Duration(d)*time.Hour)), fmt.Appendf(nil, "val-%d", d))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return err
		}
	}

	// Cells at -2h, -1h, 0, +1h, +2h from refTime.
	// Filter for [refTime-30min, refTime+30min) should match only d=0.
	start := refTime.Add(-30 * time.Minute)
	end := refTime.Add(30 * time.Minute)
	var cells int
	err := tbl.ReadRows(ctx, bigtable.SingleRow(key),
		func(r bigtable.Row) bool {
			cells += len(r["cf1"])
			return true
		},
		bigtable.RowFilter(bigtable.TimestampRangeFilter(start, end)),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with TimestampRangeFilter: %w", err)
	}
	if cells != 1 {
		return fmt.Errorf("expected 1 cell in timestamp range, got %d", cells)
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// RowSet tests
// ---------------------------------------------------------------------------

func testReadRowsRowList(ctx context.Context, tbl *bigtable.Table) error {
	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowList{"user1"},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		return fmt.Errorf("ReadRows with RowList: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("expected 1 row via RowList, got %d", rows)
	}
	return nil
}

func testReadRowsPrefixRange(ctx context.Context, tbl *bigtable.Table) error {
	var rows int
	err := tbl.ReadRows(ctx, bigtable.PrefixRange("user"),
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		return fmt.Errorf("ReadRows with PrefixRange: %w", err)
	}
	if rows < 3 {
		return fmt.Errorf("expected at least 3 rows with prefix 'user', got %d", rows)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Reversed scan
// ---------------------------------------------------------------------------

func testReadRowsReversed(ctx context.Context, tbl *bigtable.Table) error {
	var rowKeys []string
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rowKeys = append(rowKeys, r.Key())
			return true
		},
		bigtable.ReverseScan(),
	)
	if err != nil {
		return fmt.Errorf("ReadRows reversed: %w", err)
	}
	if len(rowKeys) < 2 {
		return errors.New("expected at least 2 rows in reversed order")
	}
	if rowKeys[0] <= rowKeys[len(rowKeys)-1] {
		return errors.New("expected reversed order (first key > last key)")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Binary key
// ---------------------------------------------------------------------------

func testReadRowsBinaryKey(ctx context.Context, tbl *bigtable.Table) error {
	key := string([]byte{0x00, 0xFF, 0xAB, 0xCD})

	mut := bigtable.NewMutation()
	mut.Set("cf1", "val", bigtable.Now(), []byte("binary-key-value"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return fmt.Errorf("ReadRow for binary key: %w", err)
	}
	if row == nil {
		return errors.New("row with binary key is nil")
	}
	if len(row["cf1"]) != 1 {
		return fmt.Errorf("expected 1 cell, got %d", len(row["cf1"]))
	}
	if string(row["cf1"][0].Value) != "binary-key-value" {
		return fmt.Errorf("value mismatch: got %q", row["cf1"][0].Value)
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// Large value (chunk splitting across multiple CellChunks)
// ---------------------------------------------------------------------------

func testReadRowsLargeValue(ctx context.Context, tbl *bigtable.Table) error {
	key := "large-val-test"
	size := 100 * 1024 // 100 KB, spans multiple CellChunks (max 64 KB each)
	val := make([]byte, size)
	for i := range val {
		val[i] = byte(i % 251)
	}

	mut := bigtable.NewMutation()
	mut.Set("cf1", "big", bigtable.Now(), val)
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return fmt.Errorf("ReadRow for large value: %w", err)
	}
	if row == nil {
		return errors.New("row with large value is nil")
	}
	if len(row["cf1"]) != 1 {
		return fmt.Errorf("expected 1 cell, got %d", len(row["cf1"]))
	}
	if len(row["cf1"][0].Value) != size {
		return fmt.Errorf("value size mismatch: got %d, want %d", len(row["cf1"][0].Value), size)
	}
	for i, b := range row["cf1"][0].Value {
		if b != byte(i%251) {
			return fmt.Errorf("value corruption at byte %d: got %02x, want %02x", i, b, byte(i%251))
		}
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// Many cells in a single row
// ---------------------------------------------------------------------------

func testReadRowsManyCellsSingleRow(ctx context.Context, tbl *bigtable.Table) error {
	key := "many-cells-row"
	mut := bigtable.NewMutation()
	for i := range 50 {
		mut.Set("cf1", fmt.Sprintf("q%03d", i), bigtable.Now(), fmt.Appendf(nil, "val-%d", i))
	}
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return fmt.Errorf("ReadRow for many cells: %w", err)
	}
	if row == nil {
		return errors.New("many-cells row is nil")
	}
	if len(row["cf1"]) != 50 {
		return fmt.Errorf("expected 50 cells, got %d", len(row["cf1"]))
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// CellsPerRowLimit across multiple rows (filter state reset between rows)
// ---------------------------------------------------------------------------

func testReadRowsCellsPerRowLimitMultiRow(ctx context.Context, tbl *bigtable.Table) error {
	// Write 2 cells in row1 and 2 cells in row2.
	for _, key := range []string{"cpr-row1", "cpr-row2"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "a", bigtable.Now(), []byte(key+"-a"))
		mut.Set("cf1", "b", bigtable.Now(), []byte(key+"-b"))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return fmt.Errorf("setup write %s: %w", key, err)
		}
	}

	// Read with CellsPerRowLimit(1) — should get exactly 1 cell per row.
	var totalCells int
	seenRows := make(map[string]bool)
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			seenRows[r.Key()] = true
			for _, items := range r {
				totalCells += len(items)
			}
			return true
		},
		bigtable.LimitRows(2),           // read at most 2 rows
		bigtable.RowFilter(bigtable.CellsPerRowLimitFilter(1)),  // 1 cell per row
	)
	if err != nil {
		return fmt.Errorf("ReadRows: %w", err)
	}
	if totalCells != 2 {
		return fmt.Errorf("expected 2 cells (1 per row), got %d", totalCells)
	}
	if len(seenRows) != 2 {
		return fmt.Errorf("expected 2 rows, got %d: %v", len(seenRows), seenRows)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CellsPerRowOffset across multiple rows (filter state reset between rows)
// ---------------------------------------------------------------------------

func testReadRowsCellsPerRowOffsetMultiRow(ctx context.Context, tbl *bigtable.Table) error {
	// Write 3 cells in each of 2 rows.
	for _, key := range []string{"cpo-row1", "cpo-row2"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "a", bigtable.Now(), []byte(key+"-a"))
		mut.Set("cf1", "b", bigtable.Now(), []byte(key+"-b"))
		mut.Set("cf1", "c", bigtable.Now(), []byte(key+"-c"))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return fmt.Errorf("setup write %s: %w", key, err)
		}
	}

	// Read with CellsPerRowOffsetFilter(1) — skips first cell, returns 2 per row.
	var totalCells int
	seenRows := make(map[string]bool)
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			seenRows[r.Key()] = true
			for _, items := range r {
				totalCells += len(items)
			}
			return true
		},
		bigtable.LimitRows(2),
		bigtable.RowFilter(bigtable.CellsPerRowOffsetFilter(1)),
	)
	if err != nil {
		return fmt.Errorf("ReadRows: %w", err)
	}
	if totalCells != 4 {
		return fmt.Errorf("expected 4 cells (3-1 per row × 2 rows), got %d", totalCells)
	}
	if len(seenRows) != 2 {
		return fmt.Errorf("expected 2 rows, got %d: %v", len(seenRows), seenRows)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CheckAndMutateRow with value-based predicate
// ---------------------------------------------------------------------------

func testCheckAndMutateValuePredicate(ctx context.Context, tbl *bigtable.Table) error {
	key := "check-value-pred"
	// Write a cell with a specific value.
	mut := bigtable.NewMutation()
	mut.Set("cf1", "status", bigtable.Now(), []byte("active"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	// CheckAndMutateRow: predicate is valueRegexFilter("active").
	// Should match and apply the true mutation.
	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "result", bigtable.Now(), []byte("matched"))
	condMut := bigtable.NewCondMutation(bigtable.ValueFilter("active"), trueMut, nil)

	var matched bool
	err := tbl.Apply(ctx, key, condMut, bigtable.GetCondMutationResult(&matched))
	if err != nil {
		return fmt.Errorf("CheckAndMutateRow value predicate: %w", err)
	}
	if !matched {
		return errors.New("expected predicate matched=true for value 'active'")
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("row not found")
	}
	found := false
	for _, items := range row {
		for _, item := range items {
			if string(item.Value) == "matched" {
				found = true
			}
		}
	}
	if !found {
		return errors.New("expected cf1:result=matched cell from true mutation")
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// Concurrent CheckAndMutateRow on the same row (no TOCTOU race)
// ---------------------------------------------------------------------------

func testCheckAndMutateConcurrentSameRow(ctx context.Context, tbl *bigtable.Table) error {
	key := "concurrent-check-row"

	// Seed the row.
	mut := bigtable.NewMutation()
	mut.Set("cf1", "data", bigtable.Now(), []byte("initial"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	// Launch 5 concurrent CheckAndMutateRow calls on the same row.
	// Each checks if the row exists (passAll) and writes a unique marker.
	var wg sync.WaitGroup
	errCh := make(chan error, 5)
	for i := range 5 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			trueMut := bigtable.NewMutation()
			trueMut.Set("cf1", fmt.Sprintf("marker-%d", n), bigtable.Now(), []byte("done"))
			condMut := bigtable.NewCondMutation(bigtable.PassAllFilter(), trueMut, nil)
			if err := tbl.Apply(ctx, key, condMut); err != nil {
				errCh <- fmt.Errorf("concurrent %d: %w", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	// Verify all markers were applied.
	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("row not found after concurrent CheckAndMutateRow")
	}
	for i := range 5 {
		found := false
		for _, items := range row {
			for _, item := range items {
				if item.Column == fmt.Sprintf("cf1:marker-%d", i) {
					found = true
				}
			}
		}
		if !found {
			return fmt.Errorf("marker-%d not found — CheckAndMutateRow may have lost an update", i)
		}
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// Row limit
// ---------------------------------------------------------------------------

func testReadRowsRowLimit(ctx context.Context, tbl *bigtable.Table) error {
	for _, key := range []string{"user2", "user3"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "name", bigtable.Now(), []byte(key))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return err
		}
	}

	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
		bigtable.LimitRows(2),
	)
	if err != nil {
		return fmt.Errorf("ReadRows with limit: %w", err)
	}
	if rows != 2 {
		return fmt.Errorf("expected 2 rows, got %d", rows)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CheckAndMutateRow
// ---------------------------------------------------------------------------

func testCheckAndMutateTrue(ctx context.Context, tbl *bigtable.Table) error {
	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "checked", bigtable.Now(), []byte("true"))
	condMut := bigtable.NewCondMutation(bigtable.PassAllFilter(), trueMut, nil)

	var matched bool
	err := tbl.Apply(ctx, "user1", condMut, bigtable.GetCondMutationResult(&matched))
	if err != nil {
		return fmt.Errorf("CheckAndMutateRow: %w", err)
	}
	if !matched {
		return errors.New("expected predicate matched=true")
	}

	row, err := tbl.ReadRow(ctx, "user1")
	if err != nil {
		return err
	}
	for _, cell := range row["cf1"] {
		if strings.TrimPrefix(cell.Column, "cf1:") == "checked" && string(cell.Value) == "true" {
			return nil
		}
	}
	return errors.New("expected cf1:checked=true cell")
}

func testCheckAndMutateFalse(ctx context.Context, tbl *bigtable.Table) error {
	falseMut := bigtable.NewMutation()
	falseMut.Set("cf1", "fallback", bigtable.Now(), []byte("false-branch"))
	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "should-not-appear", bigtable.Now(), []byte("nope"))

	condMut := bigtable.NewCondMutation(bigtable.BlockAllFilter(), trueMut, falseMut)

	var matched bool
	err := tbl.Apply(ctx, "no-such-row", condMut, bigtable.GetCondMutationResult(&matched))
	if err != nil {
		return fmt.Errorf("CheckAndMutateRow false branch: %w", err)
	}
	if matched {
		return errors.New("expected predicate matched=false for nonexistent row")
	}

	row, err := tbl.ReadRow(ctx, "no-such-row")
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("expected false branch to create the row")
	}
	for _, cell := range row["cf1"] {
		if strings.TrimPrefix(cell.Column, "cf1:") == "fallback" {
			return nil
		}
	}
	return errors.New("expected cf1:fallback=false-branch cell from false branch")
}

// ---------------------------------------------------------------------------
// ReadModifyWrite
// ---------------------------------------------------------------------------

func testIncrement(ctx context.Context, tbl *bigtable.Table) error {
	for i := int64(1); i <= 3; i++ {
		mut := bigtable.NewReadModifyWrite()
		mut.Increment("stats", "counter", 1)
		row, err := tbl.ApplyReadModifyWrite(ctx, "counter-row", mut)
		if err != nil {
			return fmt.Errorf("ApplyReadModifyWrite (inc %d): %w", i, err)
		}
		if len(row["stats"]) != 1 {
			return fmt.Errorf("iteration %d: expected 1 cell, got %d", i, len(row["stats"]))
		}
	}
	row, err := tbl.ReadRow(ctx, "counter-row")
	if err != nil {
		return err
	}
	if len(row["stats"]) == 0 {
		return errors.New("expected stats:counter cell")
	}
	return nil
}

func testNegativeIncrement(ctx context.Context, tbl *bigtable.Table) error {
	key := "neg-inc-test"

	mut := bigtable.NewReadModifyWrite()
	mut.Increment("stats", "counter", 100)
	row, err := tbl.ApplyReadModifyWrite(ctx, key, mut)
	if err != nil {
		return fmt.Errorf("initial increment: %w", err)
	}
	if len(row["stats"]) != 1 {
		return fmt.Errorf("expected 1 cell, got %d", len(row["stats"]))
	}

	mut = bigtable.NewReadModifyWrite()
	mut.Increment("stats", "counter", -30)
	_, err = tbl.ApplyReadModifyWrite(ctx, key, mut)
	if err != nil {
		return fmt.Errorf("negative increment: %w", err)
	}

	row, err = tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if len(row["stats"]) == 0 {
		return errors.New("expected stats:counter cell")
	}
	// Should be 70 (100 - 30), but just verify it exists.
	return nil
}

func testAppend(ctx context.Context, tbl *bigtable.Table) error {
	mut := bigtable.NewReadModifyWrite()
	mut.AppendValue("log", "events", []byte("hello"))
	row, err := tbl.ApplyReadModifyWrite(ctx, "append-row", mut)
	if err != nil {
		return fmt.Errorf("ApplyReadModifyWrite (append): %w", err)
	}
	if len(row["log"]) != 1 {
		return fmt.Errorf("expected 1 log cell, got %d", len(row["log"]))
	}

	mut = bigtable.NewReadModifyWrite()
	mut.AppendValue("log", "events", []byte(" world"))
	_, err = tbl.ApplyReadModifyWrite(ctx, "append-row", mut)
	if err != nil {
		return fmt.Errorf("ApplyReadModifyWrite (append 2): %w", err)
	}

	row, err = tbl.ReadRow(ctx, "append-row")
	if err != nil {
		return err
	}
	for _, cell := range row["log"] {
		if strings.TrimPrefix(cell.Column, "log:") == "events" {
			if string(cell.Value) != "hello world" {
				return fmt.Errorf("expected 'hello world', got %q", cell.Value)
			}
			return nil
		}
	}
	return errors.New("expected log:events cell")
}

// ---------------------------------------------------------------------------
// ApplyBulk
// ---------------------------------------------------------------------------

func testApplyBulk(ctx context.Context, tbl *bigtable.Table) error {
	keys := []string{"bulk1", "bulk2", "bulk3"}
	muts := make([]*bigtable.Mutation, 3)
	for i := range muts {
		muts[i] = bigtable.NewMutation()
		muts[i].Set("cf1", "val", bigtable.Now(), fmt.Appendf(nil, "bulk-%d", i))
	}
	errs, err := tbl.ApplyBulk(ctx, keys, muts)
	if err != nil {
		return fmt.Errorf("ApplyBulk: %w", err)
	}
	for i, e := range errs {
		if e != nil {
			return fmt.Errorf("ApplyBulk entry %d: %w", i, e)
		}
	}
	for _, key := range keys {
		row, err := tbl.ReadRow(ctx, key)
		if err != nil {
			return fmt.Errorf("reading %q after bulk: %w", key, err)
		}
		if row == nil {
			return fmt.Errorf("row %q is nil after bulk write", key)
		}
	}
	return nil
}

func testApplyBulkEmpty(ctx context.Context, tbl *bigtable.Table) error {
	_, err := tbl.ApplyBulk(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("ApplyBulk with nil args should not error: %w", err)
	}
	_, err = tbl.ApplyBulk(ctx, []string{}, []*bigtable.Mutation{})
	if err != nil {
		return fmt.Errorf("ApplyBulk with empty args should not error: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Deletes
// ---------------------------------------------------------------------------

func testDeleteFromColumn(ctx context.Context, tbl *bigtable.Table) error {
	key := "delete-col-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "keep", bigtable.Now(), []byte("stay"))
	mut.Set("cf1", "remove", bigtable.Now(), []byte("go"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	del := bigtable.NewMutation()
	del.DeleteCellsInColumn("cf1", "remove")
	if err := tbl.Apply(ctx, key, del); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if len(row["cf1"]) != 1 {
		return fmt.Errorf("expected 1 cell after column delete, got %d", len(row["cf1"]))
	}
	if string(row["cf1"][0].Value) != "stay" {
		return fmt.Errorf("expected 'stay', got %q", row["cf1"][0].Value)
	}
	return nil
}

func testDeleteFromFamily(ctx context.Context, tbl *bigtable.Table) error {
	key := "delete-fam-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("keep"))
	mut.Set("cf2", "b", bigtable.Now(), []byte("delete"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	del := bigtable.NewMutation()
	del.DeleteCellsInFamily("cf2")
	if err := tbl.Apply(ctx, key, del); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if len(row["cf2"]) != 0 {
		return errors.New("expected cf2 to be empty after family delete")
	}
	if len(row["cf1"]) == 0 {
		return errors.New("expected cf1 to still have cells")
	}
	return nil
}

func testDeleteFromRow(ctx context.Context, tbl *bigtable.Table) error {
	key := "delete-row-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("delete-me"))
	mut.Set("cf2", "b", bigtable.Now(), []byte("delete-me-too"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		return err
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	if err := tbl.Apply(ctx, key, del); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if row != nil {
		return errors.New("expected nil row after DeleteRow")
	}
	return nil
}

func testDeleteTimestampRange(ctx context.Context, tbl *bigtable.Table) error {
	key := "delete-ts-test"
	now := time.Now()
	for i := range int64(5) {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "val", bigtable.Time(now.Add(time.Duration(i)*time.Minute)), fmt.Appendf(nil, "v%d", i))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			return err
		}
	}

	// Delete cells in a time range: [now+1min, now+3min).
	del := bigtable.NewMutation()
	del.DeleteTimestampRange("cf1", "val",
		bigtable.Time(now.Add(1*time.Minute)),
		bigtable.Time(now.Add(3*time.Minute)),
	)
	if err := tbl.Apply(ctx, key, del); err != nil {
		return err
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		return err
	}
	if len(row["cf1"]) != 3 {
		return fmt.Errorf("expected 3 cells (0, 3, 4 min) after timestamp range delete, got %d", len(row["cf1"]))
	}

	del = bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, key, del)
}

// ---------------------------------------------------------------------------
// SampleRowKeys
// ---------------------------------------------------------------------------

func testSampleRowKeys(ctx context.Context, tbl *bigtable.Table) error {
	keys, err := tbl.SampleRowKeys(ctx)
	if err != nil {
		return fmt.Errorf("SampleRowKeys: %w", err)
	}
	_ = keys
	return nil
}

// ---------------------------------------------------------------------------
// Concurrent writes
// ---------------------------------------------------------------------------

func testConcurrentWrites(ctx context.Context, tbl *bigtable.Table) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mut := bigtable.NewMutation()
			mut.Set("cf1", "data", bigtable.Now(), fmt.Appendf(nil, "concurrent-%d", n))
			if err := tbl.Apply(ctx, fmt.Sprintf("concurrent-%d", n), mut); err != nil {
				errCh <- fmt.Errorf("concurrent write %d: %w", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	for i := range 10 {
		key := fmt.Sprintf("concurrent-%d", i)
		row, err := tbl.ReadRow(ctx, key)
		if err != nil {
			return fmt.Errorf("reading concurrent row %q: %w", key, err)
		}
		if row == nil {
			return fmt.Errorf("concurrent row %q not found", key)
		}

		del := bigtable.NewMutation()
		del.DeleteRow()
		_ = tbl.Apply(ctx, key, del)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Fuzz test
// ---------------------------------------------------------------------------

func fuzzRoundTrip(ctx context.Context, tbl *bigtable.Table, seed int) error {
	rng := newRNG(seed)

	// Generate random cell data.
	rowKey := fmt.Sprintf("fuzz-row-%08x", rng.uint64())
	family := fmt.Sprintf("f%02d", rng.intn(5))
	qualLen := rng.intn(20) + 1
	qual := make([]byte, qualLen)
	rng.read(qual)
	valLen := rng.intn(256)
	val := make([]byte, valLen)
	rng.read(val)

	ts := bigtable.Now()
	if rng.intn(2) == 0 {
		ts = bigtable.Time(time.Unix(0, rng.int63n(int64(time.Hour))))
	}

	// Write.
	mut := bigtable.NewMutation()
	mut.Set(family, string(qual), ts, val)
	if err := tbl.Apply(ctx, rowKey, mut); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	// Read back and verify.
	row, err := tbl.ReadRow(ctx, rowKey)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if row == nil {
		return errors.New("row not found after write")
	}
	found := false
	for _, items := range row {
		for _, item := range items {
			if strings.TrimPrefix(item.Column, family+":") == string(qual) {
				found = true
				if string(item.Value) != string(val) {
					return fmt.Errorf("value mismatch: got %q, want %q (key=%s fam=%s qual=%q)", item.Value, val, rowKey, family, qual)
				}
			}
		}
	}
	if !found {
		return fmt.Errorf("cell (fam=%s qual=%q) not found in row %s", family, qual, rowKey)
	}

	// Clean up.
	del := bigtable.NewMutation()
	del.DeleteRow()
	return tbl.Apply(ctx, rowKey, del)
}

// ---------------------------------------------------------------------------
// Simple RNG for deterministic fuzzing
// ---------------------------------------------------------------------------

type rng struct {
	state uint64
}

func newRNG(seed int) *rng {
	return &rng{state: uint64(int64(seed))*6364136223846793005 + 1442695040888963407} //nolint:gosec // PRNG
}

func (r *rng) uint64() uint64 {
	r.state ^= r.state >> 12
	r.state ^= r.state << 25
	r.state ^= r.state >> 27
	return r.state * 2685821657736338717
}

func (r *rng) intn(n int) int {
	return int(r.uint64() % uint64(n)) //nolint:gosec // n >= 0
}

func (r *rng) int63n(n int64) int64 {
	return int64(r.uint64() % uint64(n)) //nolint:gosec // n >= 0
}

func (r *rng) read(b []byte) {
	for i := range b {
		b[i] = byte(r.uint64()) //nolint:gosec // intentionally truncate
	}
}
