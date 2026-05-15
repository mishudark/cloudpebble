package bigtable

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestServer creates a Bigtable Server backed by a local objstore
// with WAL batching disabled for fast test execution.
// The server is cleaned up automatically when the test ends.
func newTestServer(t testing.TB) *Server {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")

	store, err := local.New(objDir)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{
		dir:   dir,
		store: store,
		tables: make(map[string]*tableState),
		engineOverrides: engine.Options{
			BatchWindow: -1, // disable WAL batching for speed
		},
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// openTableEngine opens the engine for the given table name using the server's
// internal helper. This is useful for direct Pebble inspection.
func openTableEngine(t testing.TB, s *Server, tableName string) *engine.Engine {
	t.Helper()
	eng, err := s.getEngine(context.Background(), tableName)
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestReadModifyWriteRowAppendMissing(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("hello"),
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Row == nil {
		t.Fatal("expected row in response")
	}
	if !bytes.Equal(resp.Row.Key, []byte("row1")) {
		t.Fatalf("row key mismatch: got %q", resp.Row.Key)
	}

	// Verify via Pebble directly.
	eng := openTableEngine(t, s, table)
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), resp.Row.Families[0].Columns[0].Cells[0].TimestampMicros)
	val, closer, err := eng.DB().Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = closer.Close() }()
	if !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("value mismatch: got %q, want %q", val, "hello")
	}
}

func TestReadModifyWriteRowAppendExisting(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	eng := openTableEngine(t, s, table)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), time.Now().UnixMicro())
	if err := db.Set(key, []byte("pre"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("-fix"),
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []byte("pre-fix")
	gotCell := resp.Row.Families[0].Columns[0].Cells[0].Value
	if !bytes.Equal(gotCell, want) {
		t.Fatalf("response value mismatch: got %q, want %q", gotCell, want)
	}

	// Verify via Pebble.
	val, closer, err := eng.DB().Get(EncodeCellKey([]byte("row1"), "cf", []byte("q"), resp.Row.Families[0].Columns[0].Cells[0].TimestampMicros))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = closer.Close() }()
	if !bytes.Equal(val, want) {
		t.Fatalf("value mismatch: got %q, want %q", val, want)
	}
}

func TestReadModifyWriteRowIncrementMissing(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("counter"),
				Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 42,
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, 42)
	gotCell := resp.Row.Families[0].Columns[0].Cells[0].Value
	if !bytes.Equal(gotCell, want) {
		t.Fatalf("response value mismatch: got %v, want %v", gotCell, want)
	}
}

func TestReadModifyWriteRowIncrementExisting(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	eng := openTableEngine(t, s, table)
	existing := make([]byte, 8)
	binary.BigEndian.PutUint64(existing, 100)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("counter"), time.Now().UnixMicro())
	if err := db.Set(key, existing, pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("counter"),
				Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 5,
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, 105)
	gotCell := resp.Row.Families[0].Columns[0].Cells[0].Value
	if !bytes.Equal(gotCell, want) {
		t.Fatalf("response value mismatch: got %v, want %v", gotCell, want)
	}
}

func TestReadModifyWriteRowIncrementInvalidSize(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	eng := openTableEngine(t, s, table)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("counter"), time.Now().UnixMicro())
	if err := db.Set(key, []byte("not-eight"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("counter"),
				Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 1,
				},
			},
		},
	}

	_, err := s.ReadModifyWriteRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for invalid value size")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", st.Code())
	}
}

func TestReadModifyWriteRowMultipleRules(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf1",
				ColumnQualifier: []byte("a"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("A"),
				},
			},
			{
				FamilyName:      "cf2",
				ColumnQualifier: []byte("b"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("B"),
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Row.Families) != 2 {
		t.Fatalf("expected 2 families, got %d", len(resp.Row.Families))
	}

	// Verify both values via Pebble.
	eng := openTableEngine(t, s, table)
	for _, fam := range resp.Row.Families {
		for _, col := range fam.Columns {
			key := EncodeCellKey([]byte("row1"), fam.Name, col.Qualifier, col.Cells[0].TimestampMicros)
			val, closer, err := eng.DB().Get(key)
			if err != nil {
				t.Fatalf("get failed: %v", err)
			}
			defer func() { _ = closer.Close() }()
			if fam.Name == "cf1" && string(col.Qualifier) == "a" {
				if !bytes.Equal(val, []byte("A")) {
					t.Fatalf("cf1:a value mismatch: got %q", val)
				}
			}
			if fam.Name == "cf2" && string(col.Qualifier) == "b" {
				if !bytes.Equal(val, []byte("B")) {
					t.Fatalf("cf2:b value mismatch: got %q", val)
				}
			}
		}
	}
}

func TestReadModifyWriteRowSequentialRules(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	eng := openTableEngine(t, s, table)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), time.Now().UnixMicro())
	if err := db.Set(key, []byte("base"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	// First appends "-1", second reads that and appends "-2".
	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("-1"),
				},
			},
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("-2"),
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []byte("base-1-2")
	gotCell := resp.Row.Families[0].Columns[0].Cells[0].Value
	if !bytes.Equal(gotCell, want) {
		t.Fatalf("sequential append mismatch: got %q, want %q", gotCell, want)
	}

	// Verify via Pebble using the new timestamp.
	newKey := EncodeCellKey([]byte("row1"), "cf", []byte("q"), resp.Row.Families[0].Columns[0].Cells[0].TimestampMicros)
	val, closer, err := eng.DB().Get(newKey)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = closer.Close() }()
	if !bytes.Equal(val, want) {
		t.Fatalf("value mismatch: got %q, want %q", val, want)
	}
}

func TestReadModifyWriteRowMissingRowKey(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: benchTable,
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("x"),
				},
			},
		},
	}

	_, err := s.ReadModifyWriteRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing row_key")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestReadModifyWriteRowMissingRules(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: benchTable,
		RowKey:    []byte("row1"),
	}

	_, err := s.ReadModifyWriteRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing rules")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestReadModifyWriteRowResponseStructure(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				Rule: &bigtablepb.ReadModifyWriteRule_AppendValue{
					AppendValue: []byte("val"),
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	row := resp.GetRow()
	if row == nil {
		t.Fatal("expected row")
	}
	if !bytes.Equal(row.Key, []byte("row1")) {
		t.Fatalf("row key mismatch")
	}
	if len(row.Families) != 1 {
		t.Fatalf("expected 1 family, got %d", len(row.Families))
	}
	fam := row.Families[0]
	if fam.Name != "cf" {
		t.Fatalf("family name mismatch: got %q", fam.Name)
	}
	if len(fam.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(fam.Columns))
	}
	col := fam.Columns[0]
	if !bytes.Equal(col.Qualifier, []byte("q")) {
		t.Fatalf("qualifier mismatch: got %q", col.Qualifier)
	}
	if len(col.Cells) != 1 {
		t.Fatalf("expected 1 cell, got %d", len(col.Cells))
	}
	cell := col.Cells[0]
	if !bytes.Equal(cell.Value, []byte("val")) {
		t.Fatalf("cell value mismatch: got %q", cell.Value)
	}
	if cell.TimestampMicros == 0 {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestReadModifyWriteRowNegativeIncrement(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := benchTable

	eng := openTableEngine(t, s, table)
	existing := make([]byte, 8)
	binary.BigEndian.PutUint64(existing, 10)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("counter"), time.Now().UnixMicro())
	if err := db.Set(key, existing, pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("counter"),
				Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: -3,
				},
			},
		},
	}

	resp, err := s.ReadModifyWriteRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, 7)
	gotCell := resp.Row.Families[0].Columns[0].Cells[0].Value
	if !bytes.Equal(gotCell, want) {
		t.Fatalf("response value mismatch: got %v, want %v", gotCell, want)
	}
}
