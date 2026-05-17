// Package clienttest verifies the official Google Cloud Bigtable client
// library works with CloudPebble's Bigtable v2 gRPC server.
//
// These are integration tests that start a real server process.
// Run with: INTEGRATION_TESTS=1 go test ./pkg/bigtable/clienttest/...
package clienttest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var repoRoot string

var (
	serverAddr   string
	sharedClient *bigtable.Client
	sharedConn   *grpc.ClientConn
	clientOnce   sync.Once
	tableID      atomic.Int64
)

func init() {
	repoRoot = findRepoRoot()
}

func findRepoRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found")
		}
		dir = parent
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION_TESTS") == "" {
		fmt.Println("skipping integration tests; set INTEGRATION_TESTS=1 to run")
		os.Exit(0)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "finding free port: %v\n", err)
		os.Exit(1)
	}
	serverAddr = lis.Addr().String()
	lis.Close()

	dd, err := os.MkdirTemp("", "pebble-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating data dir: %v\n", err)
		os.Exit(1)
	}
	od, err := os.MkdirTemp("", "objstore-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating objstore dir: %v\n", err)
		os.Exit(1)
	}

	binary := filepath.Join(repoRoot, "pebble-bigtable.testbin")
	build := exec.Command("go", "build", "-o", binary, filepath.Join(repoRoot, "cmd", "pebble-bigtable"))
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building server: %v\n%s\n", err, out)
		os.Exit(1)
	}

	cmd := exec.Command(binary, "--addr", serverAddr, "--data-dir", dd, "--object-dir", od)
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "starting server: %v\n", err)
		os.Exit(1)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "server did not start within 10s: %v\n", err)
			os.Exit(1)
		}
		time.Sleep(50 * time.Millisecond)
	}

	code := m.Run()

	if sharedClient != nil {
		sharedClient.Close()
	}
	if sharedConn != nil {
		sharedConn.Close()
	}
	syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
	cmd.Wait()

	os.Remove(binary)
	os.Exit(code)
}

func testClient(ctx context.Context, t testing.TB) *bigtable.Client {
	t.Helper()
	clientOnce.Do(func() {
		var err error
		sharedConn, err = grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dialing server: %v", err)
		}
		sharedClient, err = bigtable.NewClient(ctx, "project", "instance",
			option.WithGRPCConn(sharedConn),
		)
		if err != nil {
			t.Fatalf("creating bigtable client: %v", err)
		}
	})
	return sharedClient
}

func setupTable(ctx context.Context, t testing.TB, name string) *bigtable.Table {
	t.Helper()
	n := tableID.Add(1)
	return testClient(ctx, t).Open(fmt.Sprintf("%s-%d", name, n))
}

func TestSetCell(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-set-cell")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "email", bigtable.Now(), []byte("alice@example.com"))
	mut.Set("cf2", "score", bigtable.Now(), []byte("42"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}
}

func TestReadRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-read-row")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "email", bigtable.Now(), []byte("alice@example.com"))
	mut.Set("cf2", "score", bigtable.Now(), []byte("42"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, "user1")
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("row is nil")
	}
	if len(row["cf1"]) != 2 {
		t.Fatalf("expected 2 cells in cf1, got %d", len(row["cf1"]))
	}
	if len(row["cf2"]) != 1 {
		t.Fatalf("expected 1 cell in cf2, got %d", len(row["cf2"]))
	}
}

func TestReadRowsFullScan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-full-scan")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	if err := tbl.Apply(ctx, "row1", mut); err != nil {
		t.Fatal(err)
	}

	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if rows < 1 {
		t.Fatal("expected at least 1 row")
	}
}

func TestReadRowsRowKeyFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-rk-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
		bigtable.RowFilter(bigtable.RowKeyFilter("user1")),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected 1 row, got %d", rows)
	}
}

func TestReadRowsFamilyFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-family-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	mut.Set("cf1", "b", bigtable.Now(), []byte("y"))
	if err := tbl.Apply(ctx, "row1", mut); err != nil {
		t.Fatal(err)
	}

	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			cells += len(r["cf1"])
			return true
		},
		bigtable.RowFilter(bigtable.FamilyFilter("cf1")),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 2 {
		t.Fatalf("expected 2 cells in cf1, got %d", cells)
	}
}

func TestReadRowsColumnFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-col-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "email", bigtable.Now(), []byte("alice@example.com"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	var cells int
	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, fam := range r {
				cells += len(fam)
			}
			return true
		},
		bigtable.RowFilter(bigtable.ColumnFilter("name")),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 1 {
		t.Fatalf("expected 1 cell matching column 'name', got %d", cells)
	}
}

func TestReadRowsValueFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-value-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "score", bigtable.Now(), []byte("42"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 1 {
		t.Fatalf("expected 1 cell with value '42', got %d", cells)
	}
}

func TestReadRowsChainFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-chain-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	mut.Set("cf1", "email", bigtable.Now(), []byte("alice@example.com"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 1 {
		t.Fatalf("expected 1 cell matching cf1 AND name, got %d", cells)
	}
}

func TestReadRowsInterleaveFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-interleave-filter")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	mut.Set("cf1", "b", bigtable.Now(), []byte("y"))
	mut.Set("cf2", "c", bigtable.Now(), []byte("z"))
	if err := tbl.Apply(ctx, "row1", mut); err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 3 {
		t.Fatalf("expected 3 cells across cf1 OR cf2, got %d", cells)
	}
}

func TestReadRowsStripValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-strip-value")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""),
		func(r bigtable.Row) bool {
			for _, items := range r {
				for _, item := range items {
					if len(item.Value) != 0 {
						t.Errorf("expected empty value with strip value filter, got %q", item.Value)
						return false
					}
				}
			}
			return true
		},
		bigtable.RowFilter(bigtable.StripValueFilter()),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
}

func TestReadRowsLatestNFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-latest-n")

	key := "latest-n-test"
	for i := int64(1); i <= 5; i++ {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "ver", bigtable.Time(time.Unix(0, i*int64(time.Millisecond))), fmt.Appendf(nil, "v%d", i))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
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
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 2 {
		t.Fatalf("expected 2 cells with LatestNFilter(2), got %d", cells)
	}
}

func TestReadRowsTimestampRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-ts-range")

	key := "ts-range-test"
	refTime := time.Now()
	for d := -2; d <= 2; d++ {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "val", bigtable.Time(refTime.Add(time.Duration(d)*time.Hour)), fmt.Appendf(nil, "val-%d", d))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
		}
	}

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
		t.Fatalf("ReadRows: %v", err)
	}
	if cells != 1 {
		t.Fatalf("expected 1 cell in timestamp range, got %d", cells)
	}
}

func TestReadRowsRowList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-row-list")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	var rows int
	err := tbl.ReadRows(ctx, bigtable.RowList{"user1"},
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected 1 row via RowList, got %d", rows)
	}
}

func TestReadRowsPrefixRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-prefix-range")

	for _, key := range []string{"user1", "user2", "user3"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "name", bigtable.Now(), []byte(key))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
		}
	}

	var rows int
	err := tbl.ReadRows(ctx, bigtable.PrefixRange("user"),
		func(r bigtable.Row) bool {
			rows++
			return true
		},
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if rows < 3 {
		t.Fatalf("expected at least 3 rows with prefix 'user', got %d", rows)
	}
}

func TestReadRowsReversed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-reversed")

	for _, key := range []string{"aaa", "bbb", "ccc"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "a", bigtable.Now(), []byte(key))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
		}
	}

	var rowKeys []string
	err := tbl.ReadRows(ctx, bigtable.RowRange{},
		func(r bigtable.Row) bool {
			rowKeys = append(rowKeys, r.Key())
			return true
		},
		bigtable.ReverseScan(),
	)
	if err != nil {
		t.Fatalf("ReadRows reversed: %v", err)
	}
	if len(rowKeys) < 2 {
		t.Fatal("expected at least 2 rows in reversed order")
	}
	if rowKeys[0] <= rowKeys[len(rowKeys)-1] {
		t.Fatal("expected reversed order (first key > last key)")
	}
}

func TestReadRowsBinaryKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-binary-key")

	key := string([]byte{0x00, 0xFF, 0xAB, 0xCD})
	mut := bigtable.NewMutation()
	mut.Set("cf1", "val", bigtable.Now(), []byte("binary-key-value"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("row is nil")
	}
	if len(row["cf1"]) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(row["cf1"]))
	}
	if string(row["cf1"][0].Value) != "binary-key-value" {
		t.Fatalf("value mismatch: got %q", row["cf1"][0].Value)
	}
}

func TestReadRowsLargeValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-large-value")

	key := "large-val-test"
	size := 100 * 1024
	val := make([]byte, size)
	for i := range val {
		val[i] = byte(i % 251)
	}

	mut := bigtable.NewMutation()
	mut.Set("cf1", "big", bigtable.Now(), val)
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("row is nil")
	}
	if len(row["cf1"]) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(row["cf1"]))
	}
	if len(row["cf1"][0].Value) != size {
		t.Fatalf("value size mismatch: got %d, want %d", len(row["cf1"][0].Value), size)
	}
	for i, b := range row["cf1"][0].Value {
		if b != byte(i%251) {
			t.Fatalf("value corruption at byte %d: got %02x, want %02x", i, b, byte(i%251))
		}
	}
}

func TestReadRowsManyCellsSingleRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-many-cells")

	key := "many-cells-row"
	mut := bigtable.NewMutation()
	for i := range 50 {
		mut.Set("cf1", fmt.Sprintf("q%03d", i), bigtable.Now(), fmt.Appendf(nil, "val-%d", i))
	}
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("row is nil")
	}
	if len(row["cf1"]) != 50 {
		t.Fatalf("expected 50 cells, got %d", len(row["cf1"]))
	}
}

func TestReadRowsCellsPerRowLimitMultiRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cpr-limit")

	for _, key := range []string{"cpr-row1", "cpr-row2"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "a", bigtable.Now(), []byte(key+"-a"))
		mut.Set("cf1", "b", bigtable.Now(), []byte(key+"-b"))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatalf("setup write %s: %v", key, err)
		}
	}

	var totalCells int
	seenRows := make(map[string]bool)
	err := tbl.ReadRows(ctx, bigtable.PrefixRange("cpr"),
		func(r bigtable.Row) bool {
			seenRows[r.Key()] = true
			for _, items := range r {
				totalCells += len(items)
			}
			return true
		},
		bigtable.RowFilter(bigtable.CellsPerRowLimitFilter(1)),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if totalCells != 2 {
		t.Fatalf("expected 2 cells (1 per row), got %d", totalCells)
	}
	if len(seenRows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(seenRows), seenRows)
	}
}

func TestReadRowsCellsPerRowOffsetMultiRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cpo-offset")

	for _, key := range []string{"cpo-row1", "cpo-row2"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "a", bigtable.Now(), []byte(key+"-a"))
		mut.Set("cf1", "b", bigtable.Now(), []byte(key+"-b"))
		mut.Set("cf1", "c", bigtable.Now(), []byte(key+"-c"))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatalf("setup write %s: %v", key, err)
		}
	}

	var totalCells int
	seenRows := make(map[string]bool)
	err := tbl.ReadRows(ctx, bigtable.PrefixRange("cpo"),
		func(r bigtable.Row) bool {
			seenRows[r.Key()] = true
			for _, items := range r {
				totalCells += len(items)
			}
			return true
		},
		bigtable.RowFilter(bigtable.CellsPerRowOffsetFilter(1)),
	)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if totalCells != 4 {
		t.Fatalf("expected 4 cells (3-1 per row × 2 rows), got %d", totalCells)
	}
	if len(seenRows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(seenRows), seenRows)
	}
}

func TestCheckAndMutateRowTrueBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cam-true")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "name", bigtable.Now(), []byte("Alice"))
	if err := tbl.Apply(ctx, "user1", mut); err != nil {
		t.Fatal(err)
	}

	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "checked", bigtable.Now(), []byte("true"))

	var matched bool
	err := tbl.Apply(ctx, "user1", bigtable.NewCondMutation(bigtable.PassAllFilter(), trueMut, nil), bigtable.GetCondMutationResult(&matched))
	if err != nil {
		t.Fatalf("CheckAndMutateRow: %v", err)
	}
	if !matched {
		t.Fatal("expected predicate matched=true")
	}

	row, err := tbl.ReadRow(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range row["cf1"] {
		if strings.TrimPrefix(cell.Column, "cf1:") == "checked" && string(cell.Value) == "true" {
			return
		}
	}
	t.Fatal("expected cf1:checked=true cell")
}

func TestCheckAndMutateRowFalseBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cam-false")

	falseMut := bigtable.NewMutation()
	falseMut.Set("cf1", "fallback", bigtable.Now(), []byte("false-branch"))
	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "should-not-appear", bigtable.Now(), []byte("nope"))

	var matched bool
	err := tbl.Apply(ctx, "no-such-row", bigtable.NewCondMutation(bigtable.BlockAllFilter(), trueMut, falseMut), bigtable.GetCondMutationResult(&matched))
	if err != nil {
		t.Fatalf("CheckAndMutateRow: %v", err)
	}
	if matched {
		t.Fatal("expected predicate matched=false")
	}

	row, err := tbl.ReadRow(ctx, "no-such-row")
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected false branch to create the row")
	}
	for _, cell := range row["cf1"] {
		if strings.TrimPrefix(cell.Column, "cf1:") == "fallback" {
			return
		}
	}
	t.Fatal("expected cf1:fallback=false-branch cell")
}

func TestReadModifyWriteIncrement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-rmw-inc")

	for i := int64(1); i <= 3; i++ {
		mut := bigtable.NewReadModifyWrite()
		mut.Increment("stats", "counter", 1)
		row, err := tbl.ApplyReadModifyWrite(ctx, "counter-row", mut)
		if err != nil {
			t.Fatalf("ApplyReadModifyWrite (inc %d): %v", i, err)
		}
		if len(row["stats"]) != 1 {
			t.Fatalf("iteration %d: expected 1 cell, got %d", i, len(row["stats"]))
		}
	}
}

func TestReadModifyWriteNegativeIncrement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-rmw-neginc")

	key := "neg-inc-test"

	mut := bigtable.NewReadModifyWrite()
	mut.Increment("stats", "counter", 100)
	row, err := tbl.ApplyReadModifyWrite(ctx, key, mut)
	if err != nil {
		t.Fatalf("initial increment: %v", err)
	}
	if len(row["stats"]) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(row["stats"]))
	}

	mut = bigtable.NewReadModifyWrite()
	mut.Increment("stats", "counter", -30)
	_, err = tbl.ApplyReadModifyWrite(ctx, key, mut)
	if err != nil {
		t.Fatalf("negative increment: %v", err)
	}

	row, err = tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(row["stats"]) == 0 {
		t.Fatal("expected stats:counter cell")
	}
}

func TestReadModifyWriteAppend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-rmw-append")

	mut := bigtable.NewReadModifyWrite()
	mut.AppendValue("log", "events", []byte("hello"))
	row, err := tbl.ApplyReadModifyWrite(ctx, "append-row", mut)
	if err != nil {
		t.Fatalf("ApplyReadModifyWrite (append): %v", err)
	}
	if len(row["log"]) != 1 {
		t.Fatalf("expected 1 log cell, got %d", len(row["log"]))
	}

	mut = bigtable.NewReadModifyWrite()
	mut.AppendValue("log", "events", []byte(" world"))
	_, err = tbl.ApplyReadModifyWrite(ctx, "append-row", mut)
	if err != nil {
		t.Fatalf("ApplyReadModifyWrite (append 2): %v", err)
	}

	row, err = tbl.ReadRow(ctx, "append-row")
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range row["log"] {
		if strings.TrimPrefix(cell.Column, "log:") == "events" {
			if string(cell.Value) != "hello world" {
				t.Fatalf("expected 'hello world', got %q", cell.Value)
			}
			return
		}
	}
	t.Fatal("expected log:events cell")
}

func TestApplyBulk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-apply-bulk")

	keys := []string{"bulk1", "bulk2", "bulk3"}
	muts := make([]*bigtable.Mutation, 3)
	for i := range muts {
		muts[i] = bigtable.NewMutation()
		muts[i].Set("cf1", "val", bigtable.Now(), fmt.Appendf(nil, "bulk-%d", i))
	}

	errs, err := tbl.ApplyBulk(ctx, keys, muts)
	if err != nil {
		t.Fatalf("ApplyBulk: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("ApplyBulk entry %d: %v", i, e)
		}
	}
	for _, key := range keys {
		row, err := tbl.ReadRow(ctx, key)
		if err != nil {
			t.Fatalf("reading %q after bulk: %v", key, err)
		}
		if row == nil {
			t.Fatalf("row %q is nil after bulk write", key)
		}
	}
}

func TestApplyBulkEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-apply-bulk-empty")

	_, err := tbl.ApplyBulk(ctx, nil, nil)
	if err != nil {
		t.Fatalf("ApplyBulk with nil args: %v", err)
	}
	_, err = tbl.ApplyBulk(ctx, []string{}, []*bigtable.Mutation{})
	if err != nil {
		t.Fatalf("ApplyBulk with empty args: %v", err)
	}
}

func TestDeleteFromColumn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-delete-col")

	key := "delete-col-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "keep", bigtable.Now(), []byte("stay"))
	mut.Set("cf1", "remove", bigtable.Now(), []byte("go"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	del := bigtable.NewMutation()
	del.DeleteCellsInColumn("cf1", "remove")
	if err := tbl.Apply(ctx, key, del); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(row["cf1"]) != 1 {
		t.Fatalf("expected 1 cell after column delete, got %d", len(row["cf1"]))
	}
	if string(row["cf1"][0].Value) != "stay" {
		t.Fatalf("expected 'stay', got %q", row["cf1"][0].Value)
	}
}

func TestDeleteFromFamily(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-delete-fam")

	key := "delete-fam-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("keep"))
	mut.Set("cf2", "b", bigtable.Now(), []byte("delete"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	del := bigtable.NewMutation()
	del.DeleteCellsInFamily("cf2")
	if err := tbl.Apply(ctx, key, del); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(row["cf2"]) != 0 {
		t.Fatal("expected cf2 to be empty after family delete")
	}
	if len(row["cf1"]) == 0 {
		t.Fatal("expected cf1 to still have cells")
	}
}

func TestDeleteFromRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-delete-row")

	key := "delete-row-test"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("delete-me"))
	mut.Set("cf2", "b", bigtable.Now(), []byte("delete-me-too"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatal(err)
	}

	del := bigtable.NewMutation()
	del.DeleteRow()
	if err := tbl.Apply(ctx, key, del); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Fatal("expected nil row after DeleteRow")
	}
}

func TestDeleteTimestampRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-delete-ts")

	key := "delete-ts-test"
	now := time.Now()
	for i := range int64(5) {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "val", bigtable.Time(now.Add(time.Duration(i)*time.Minute)), fmt.Appendf(nil, "v%d", i))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
		}
	}

	del := bigtable.NewMutation()
	del.DeleteTimestampRange("cf1", "val",
		bigtable.Time(now.Add(1*time.Minute)),
		bigtable.Time(now.Add(3*time.Minute)),
	)
	if err := tbl.Apply(ctx, key, del); err != nil {
		t.Fatal(err)
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(row["cf1"]) != 3 {
		t.Fatalf("expected 3 cells after timestamp range delete, got %d", len(row["cf1"]))
	}
}

func TestSampleRowKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-sample-keys")

	mut := bigtable.NewMutation()
	mut.Set("cf1", "a", bigtable.Now(), []byte("x"))
	if err := tbl.Apply(ctx, "row1", mut); err != nil {
		t.Fatal(err)
	}

	keys, err := tbl.SampleRowKeys(ctx)
	if err != nil {
		t.Fatalf("SampleRowKeys: %v", err)
	}
	_ = keys
}

func TestConcurrentWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-concurrent-writes")

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mut := bigtable.NewMutation()
			mut.Set("cf1", "data", bigtable.Now(), fmt.Appendf(nil, "concurrent-%d", n))
			if err := tbl.Apply(ctx, fmt.Sprintf("concurrent-%d", n), mut); err != nil {
				errCh <- fmt.Errorf("concurrent write %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := range 10 {
		key := fmt.Sprintf("concurrent-%d", i)
		row, err := tbl.ReadRow(ctx, key)
		if err != nil {
			t.Fatalf("reading concurrent row %q: %v", key, err)
		}
		if row == nil {
			t.Fatalf("concurrent row %q not found", key)
		}
	}
}

func TestCheckAndMutateValuePredicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cam-value-pred")

	key := "check-value-pred"
	mut := bigtable.NewMutation()
	mut.Set("cf1", "status", bigtable.Now(), []byte("active"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatalf("setup: %v", err)
	}

	trueMut := bigtable.NewMutation()
	trueMut.Set("cf1", "result", bigtable.Now(), []byte("matched"))
	condMut := bigtable.NewCondMutation(bigtable.ValueFilter("active"), trueMut, nil)

	var matched bool
	err := tbl.Apply(ctx, key, condMut, bigtable.GetCondMutationResult(&matched))
	if err != nil {
		t.Fatalf("CheckAndMutateRow: %v", err)
	}
	if !matched {
		t.Fatal("expected predicate matched=true")
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("row not found")
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
		t.Fatal("expected cf1:result=matched cell")
	}
}

func TestCheckAndMutateConcurrentSameRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-cam-concurrent")

	key := "concurrent-check-row"

	mut := bigtable.NewMutation()
	mut.Set("cf1", "data", bigtable.Now(), []byte("initial"))
	if err := tbl.Apply(ctx, key, mut); err != nil {
		t.Fatalf("setup: %v", err)
	}

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
				errCh <- fmt.Errorf("concurrent %d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	row, err := tbl.ReadRow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("row not found after concurrent CheckAndMutateRow")
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
			t.Fatalf("marker-%d not found — CheckAndMutateRow may have lost an update", i)
		}
	}
}

func TestReadRowsRowLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tbl := setupTable(ctx, t, "test-row-limit")

	for _, key := range []string{"user2", "user3"} {
		mut := bigtable.NewMutation()
		mut.Set("cf1", "name", bigtable.Now(), []byte(key))
		if err := tbl.Apply(ctx, key, mut); err != nil {
			t.Fatal(err)
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
		t.Fatalf("ReadRows with limit: %v", err)
	}
	if rows != 2 {
		t.Fatalf("expected 2 rows, got %d", rows)
	}
}

func TestFuzzRoundTrip(t *testing.T) {
	t.Parallel()
	rng := newRNG(42)
	tbl := setupTable(context.Background(), t, "test-fuzz")

	ctx := context.Background()
	for i := range 20 {
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

		mut := bigtable.NewMutation()
		mut.Set(family, string(qual), ts, val)
		if err := tbl.Apply(ctx, rowKey, mut); err != nil {
			t.Fatalf("iteration %d write: %v", i, err)
		}

		row, err := tbl.ReadRow(ctx, rowKey)
		if err != nil {
			t.Fatalf("iteration %d read: %v", i, err)
		}
		if row == nil {
			t.Fatalf("iteration %d: row not found after write", i)
		}
		found := false
		for _, items := range row {
			for _, item := range items {
				if strings.TrimPrefix(item.Column, family+":") == string(qual) {
					found = true
					if string(item.Value) != string(val) {
						t.Fatalf("iteration %d: value mismatch: got %q, want %q", i, item.Value, val)
					}
				}
			}
		}
		if !found {
			t.Fatalf("iteration %d: cell (fam=%s qual=%q) not found", i, family, qual)
		}
	}
}

type rng struct {
	state uint64
}

func newRNG(seed int) *rng {
	return &rng{state: uint64(int64(seed))*6364136223846793005 + 1442695040888963407}
}

func (r *rng) uint64() uint64 {
	r.state ^= r.state >> 12
	r.state ^= r.state << 25
	r.state ^= r.state >> 27
	return r.state * 2685821657736338717
}

func (r *rng) intn(n int) int {
	return int(r.uint64() % uint64(n))
}

func (r *rng) int63n(n int64) int64 {
	return int64(r.uint64() % uint64(n))
}

func (r *rng) read(b []byte) {
	for i := range b {
		b[i] = byte(r.uint64())
	}
}
