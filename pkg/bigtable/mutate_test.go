package bigtable

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// mockServerStream implements grpc.ServerStreamingServer for testing.
// T must be a pointer type (e.g. *bigtablepb.MutateRowsResponse).
type mockServerStream[T any] struct {
	ctx    context.Context
	sent   []T
	sendFn func(T) error
}

func newMockServerStream[T any]() *mockServerStream[T] {
	return &mockServerStream[T]{ctx: context.Background()}
}

func (m *mockServerStream[T]) Send(resp T) error {
	m.sent = append(m.sent, resp)
	if m.sendFn != nil {
		return m.sendFn(resp)
	}
	return nil
}
func (m *mockServerStream[T]) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream[T]) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream[T]) SetTrailer(metadata.MD)       {}
func (m *mockServerStream[T]) Context() context.Context     { return m.ctx }
func (m *mockServerStream[T]) SendMsg(any) error            { return nil }
func (m *mockServerStream[T]) RecvMsg(any) error            { return io.EOF }

func TestMutateRowSetCell(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
						TimestampMicros: 1000000,
						Value:           []byte("hello"),
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via Pebble.
	eng := openTableEngine(t, s, table)
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 1000000)
	val, closer, err := eng.DB().Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("value mismatch: got %q, want %q", val, "hello")
	}
}

func TestMutateRowSetCellDefaultTimestamp(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	before := time.Now().UnixMicro()
	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
						TimestampMicros: -1,
						Value:           []byte("auto-ts"),
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify via Pebble - decode to check timestamp was assigned.
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	start, end := rowKeyColumnBounds([]byte("row1"), "cf", []byte("q"))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if !iter.First() {
		t.Fatal("expected at least one cell")
	}
	_, _, _, ts, ok := DecodeCellKey(iter.Key())
	if !ok {
		t.Fatal("failed to decode key")
	}
	if ts < before {
		t.Fatalf("expected timestamp >= %d, got %d", before, ts)
	}
}

func TestMutateRowDeleteFromColumn(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	// Write a cell first.
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 1000000)
	if err := db.Set(key, []byte("delete-me"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_DeleteFromColumn_{
					DeleteFromColumn: &bigtablepb.Mutation_DeleteFromColumn{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cell should be deleted.
	_, closer, err := db.Get(key)
	if err != pebble.ErrNotFound {
		if closer != nil {
			closer.Close()
		}
		t.Fatal("expected cell to be deleted")
	}
}

func TestMutateRowDeleteFromColumnWithTimeRange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key1 := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 100)
	key2 := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 300)
	if err := db.Set(key1, []byte("v1"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(key2, []byte("v2"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_DeleteFromColumn_{
					DeleteFromColumn: &bigtablepb.Mutation_DeleteFromColumn{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
						TimeRange: &bigtablepb.TimestampRange{
							StartTimestampMicros: 50,
							EndTimestampMicros:   200,
						},
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// key1 (ts=100) should be deleted.
	_, closer, err := db.Get(key1)
	if err != pebble.ErrNotFound {
		if closer != nil {
			closer.Close()
		}
		t.Fatal("expected ts=100 cell to be deleted")
	}

	// key2 (ts=300) should still exist.
	val, closer, err := db.Get(key2)
	if err != nil {
		t.Fatalf("expected ts=300 cell to exist: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("value mismatch: got %q", val)
	}
}

func TestMutateRowDeleteFromFamily(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key1 := EncodeCellKey([]byte("row1"), "cf1", []byte("q"), 100)
	key2 := EncodeCellKey([]byte("row1"), "cf2", []byte("q"), 100)
	if err := db.Set(key1, []byte("keep"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(key2, []byte("delete"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_DeleteFromFamily_{
					DeleteFromFamily: &bigtablepb.Mutation_DeleteFromFamily{
						FamilyName: "cf2",
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// cf1 cell should exist.
	val, closer, err := db.Get(key1)
	if err != nil {
		t.Fatalf("expected cf1 cell to exist: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(val, []byte("keep")) {
		t.Fatalf("value mismatch: got %q", val)
	}

	// cf2 cell should be deleted.
	_, closer2, err := db.Get(key2)
	if err != pebble.ErrNotFound {
		if closer2 != nil {
			closer2.Close()
		}
		t.Fatal("expected cf2 cell to be deleted")
	}
}

func TestMutateRowDeleteFromRow(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	eng := openTableEngine(t, s, table)
	db := eng.DB()

	for _, fam := range []string{"cf1", "cf2"} {
		key := EncodeCellKey([]byte("row1"), fam, []byte("q"), 100)
		if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_DeleteFromRow_{
					DeleteFromRow: &bigtablepb.Mutation_DeleteFromRow{},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All cells in row1 should be deleted.
	start, end := rowKeyRangeBounds([]byte("row1"))
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if iter.First() {
		t.Fatal("expected no cells in row1")
	}
}

func TestMutateRowMissingRowKey(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
						Value:           []byte("x"),
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing row_key")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestMutateRowMissingMutations(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("row1"),
	}

	_, err := s.MutateRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing mutations")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestMutateRowUnimplementedAddToCell(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_AddToCell_{
					AddToCell: &bigtablepb.Mutation_AddToCell{},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for AddToCell")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}
}

func TestMutateRowUnimplementedMergeToCell(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.MutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_MergeToCell_{
					MergeToCell: &bigtablepb.Mutation_MergeToCell{},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for MergeToCell")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}
}

func TestMutateRowMultipleMutations(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf1",
						ColumnQualifier: []byte("a"),
						TimestampMicros: 100,
						Value:           []byte("v1"),
					},
				},
			},
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf2",
						ColumnQualifier: []byte("b"),
						TimestampMicros: 200,
						Value:           []byte("v2"),
					},
				},
			},
		},
	}

	_, err := s.MutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key1 := EncodeCellKey([]byte("row1"), "cf1", []byte("a"), 100)
	val1, closer1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer1.Close()
	if !bytes.Equal(val1, []byte("v1")) {
		t.Fatalf("value mismatch: got %q", val1)
	}

	key2 := EncodeCellKey([]byte("row1"), "cf2", []byte("b"), 200)
	val2, closer2, err := db.Get(key2)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer2.Close()
	if !bytes.Equal(val2, []byte("v2")) {
		t.Fatalf("value mismatch: got %q", val2)
	}
}

func TestMutateRowsSuccess(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.MutateRowsRequest{
		TableName: table,
		Entries: []*bigtablepb.MutateRowsRequest_Entry{
			{
				RowKey: []byte("row1"),
				Mutations: []*bigtablepb.Mutation{
					{
						Mutation: &bigtablepb.Mutation_SetCell_{
							SetCell: &bigtablepb.Mutation_SetCell{
								FamilyName:      "cf",
								ColumnQualifier: []byte("a"),
								TimestampMicros: 100,
								Value:           []byte("v1"),
							},
						},
					},
				},
			},
			{
				RowKey: []byte("row2"),
				Mutations: []*bigtablepb.Mutation{
					{
						Mutation: &bigtablepb.Mutation_SetCell_{
							SetCell: &bigtablepb.Mutation_SetCell{
								FamilyName:      "cf",
								ColumnQualifier: []byte("b"),
								TimestampMicros: 200,
								Value:           []byte("v2"),
							},
						},
					},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.MutateRowsResponse]()
	err := s.MutateRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stream.sent) != 1 {
		t.Fatalf("expected 1 response, got %d", len(stream.sent))
	}

	resp := stream.sent[0]
	if len(resp.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(resp.Entries))
	}
	for _, e := range resp.Entries {
		if e.Status.Code != 0 { // OK
			t.Fatalf("entry %d: expected OK, got code %d", e.Index, e.Status.Code)
		}
	}

	// Verify data in Pebble.
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key1 := EncodeCellKey([]byte("row1"), "cf", []byte("a"), 100)
	val1, closer1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer1.Close()
	if !bytes.Equal(val1, []byte("v1")) {
		t.Fatalf("value mismatch: got %q", val1)
	}
}

func TestMutateRowsEmptyEntries(t *testing.T) {
	s := newTestServer(t)

	req := &bigtablepb.MutateRowsRequest{
		TableName: "projects/p/instances/i/tables/t",
	}

	stream := newMockServerStream[*bigtablepb.MutateRowsResponse]()
	err := s.MutateRows(req, stream)
	if err == nil {
		t.Fatal("expected error for empty entries")
	}
}

func TestMutateRowsPartialFailure(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.MutateRowsRequest{
		TableName: table,
		Entries: []*bigtablepb.MutateRowsRequest_Entry{
			{
				RowKey: []byte("row1"),
				Mutations: []*bigtablepb.Mutation{
					{
						Mutation: &bigtablepb.Mutation_SetCell_{
							SetCell: &bigtablepb.Mutation_SetCell{
								FamilyName:      "cf",
								ColumnQualifier: []byte("a"),
								TimestampMicros: 100,
								Value:           []byte("v1"),
							},
						},
					},
				},
			},
			{
				RowKey: []byte("row2"),
				Mutations: []*bigtablepb.Mutation{
					{
						Mutation: &bigtablepb.Mutation_AddToCell_{
							AddToCell: &bigtablepb.Mutation_AddToCell{},
						},
					},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.MutateRowsResponse]()
	err := s.MutateRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stream.sent) != 1 {
		t.Fatalf("expected 1 response, got %d", len(stream.sent))
	}

	resp := stream.sent[0]
	if len(resp.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(resp.Entries))
	}

	// Check by index field (entries may be in any order).
	byIndex := make(map[int64]*bigtablepb.MutateRowsResponse_Entry)
	for _, e := range resp.Entries {
		byIndex[e.Index] = e
	}
	if e, ok := byIndex[0]; !ok || e.Status.Code != 0 {
		t.Fatal("expected entry 0 to be OK")
	}
	if e, ok := byIndex[1]; !ok || e.Status.Code == 0 {
		t.Fatal("expected entry 1 to have error")
	}
}

func TestCheckAndMutateRowPredicateMatched(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	// Pre-write a cell so predicate matches.
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 100)
	if err := db.Set(key, []byte("exists"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.CheckAndMutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		PredicateFilter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_PassAllFilter{},
		},
		TrueMutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("result"),
						TimestampMicros: 200,
						Value:           []byte("true-branch"),
					},
				},
			},
		},
		FalseMutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("result"),
						TimestampMicros: 200,
						Value:           []byte("false-branch"),
					},
				},
			},
		},
	}

	resp, err := s.CheckAndMutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.PredicateMatched {
		t.Fatal("expected predicate to match")
	}

	// Verify true-branch was applied.
	resultKey := EncodeCellKey([]byte("row1"), "cf", []byte("result"), 200)
	val, closer, err := db.Get(resultKey)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(val, []byte("true-branch")) {
		t.Fatalf("expected true-branch value, got %q", val)
	}
}

func TestCheckAndMutateRowPredicateNotMatched(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	// No cells in row1 = predicate won't match (row is empty).
	req := &bigtablepb.CheckAndMutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		PredicateFilter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_PassAllFilter{},
		},
		TrueMutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("result"),
						TimestampMicros: 200,
						Value:           []byte("true-branch"),
					},
				},
			},
		},
		FalseMutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("result"),
						TimestampMicros: 200,
						Value:           []byte("false-branch"),
					},
				},
			},
		},
	}

	resp, err := s.CheckAndMutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.PredicateMatched {
		t.Fatal("expected predicate to not match")
	}

	// Verify false-branch was applied.
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	resultKey := EncodeCellKey([]byte("row1"), "cf", []byte("result"), 200)
	val, closer, err := db.Get(resultKey)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(val, []byte("false-branch")) {
		t.Fatalf("expected false-branch value, got %q", val)
	}
}

func TestCheckAndMutateRowNoMutations(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.CheckAndMutateRowRequest{
		TableName: table,
		RowKey:    []byte("row1"),
		PredicateFilter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_PassAllFilter{},
		},
	}

	resp, err := s.CheckAndMutateRow(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No cells in row1, predicate shouldn't match (filter passes but no cells to evaluate).
	if resp.PredicateMatched {
		t.Fatal("expected predicate to not match on empty row")
	}
}

func TestCheckAndMutateRowMissingRowKey(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.CheckAndMutateRowRequest{
		TableName: "projects/p/instances/i/tables/t",
	}

	_, err := s.CheckAndMutateRow(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing row_key")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestRowHasCells(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 100)
	if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	if !rowHasCells(db, []byte("row1"), nil) {
		t.Fatal("expected rowHasCells to return true")
	}
	if rowHasCells(db, []byte("row2"), nil) {
		t.Fatal("expected rowHasCells to return false for missing row")
	}
}

func TestRowHasCellsWithFilter(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 100)
	if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}

	filter := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_PassAllFilter{},
	}
	if !rowHasCells(db, []byte("row1"), filter) {
		t.Fatal("expected rowHasCells with passAll filter to return true")
	}

	blockFilter := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_BlockAllFilter{},
	}
	if rowHasCells(db, []byte("row1"), blockFilter) {
		t.Fatal("expected rowHasCells with blockAll filter to return false")
	}
}

func TestToBigtableStatus(t *testing.T) {
	s := status.New(codes.InvalidArgument, "bad input")
	bt := toBigtableStatus(s.Err())
	if bt.Code != int32(codes.InvalidArgument) {
		t.Fatalf("code mismatch: got %d", bt.Code)
	}
}

func TestOkStatus(t *testing.T) {
	s := okStatus()
	if s.Code != 0 {
		t.Fatalf("expected 0, got %d", s.Code)
	}
}
