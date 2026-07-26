package bigtable

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func addToCellMutation(family string, qualifier []byte, delta int64) *bigtablepb.Mutation {
	return &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_AddToCell_{
			AddToCell: &bigtablepb.Mutation_AddToCell{
				FamilyName: family,
				ColumnQualifier: &bigtablepb.Value{
					Kind: &bigtablepb.Value_RawValue{RawValue: qualifier},
				},
				Input: &bigtablepb.Value{
					Kind: &bigtablepb.Value_IntValue{IntValue: delta},
				},
			},
		},
	}
}

func mergeToCellMutation(family string, qualifier []byte, input *bigtablepb.Value) *bigtablepb.Mutation {
	return &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_MergeToCell_{
			MergeToCell: &bigtablepb.Mutation_MergeToCell{
				FamilyName: family,
				ColumnQualifier: &bigtablepb.Value{
					Kind: &bigtablepb.Value_RawValue{RawValue: qualifier},
				},
				Input: input,
			},
		},
	}
}

func mutateRow(t *testing.T, s *Server, table string, rowKey []byte, mutations ...*bigtablepb.Mutation) error {
	t.Helper()
	_, err := s.MutateRow(context.Background(), &bigtablepb.MutateRowRequest{
		TableName: table,
		RowKey:    rowKey,
		Mutations: mutations,
	})
	return err
}

func readAggregate(t *testing.T, s *Server, table string, rowKey []byte, family string, qualifier []byte) (int64, bool) {
	t.Helper()
	eng := openTableEngine(t, s, table)
	val := readCellValue(eng.DB(), rowKey, family, qualifier)
	if val == nil {
		return 0, false
	}
	if len(val) != 8 {
		t.Fatalf("aggregate cell value is %d bytes, expected 8", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), true
}

func TestAddToCellBasic(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 3)); err != nil {
		t.Fatal(err)
	}
	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 7)); err != nil {
		t.Fatal(err)
	}

	got, ok := readAggregate(t, s, table, row, "cf", []byte("hits"))
	if !ok {
		t.Fatal("aggregate cell not found")
	}
	if got != 10 {
		t.Fatalf("expected sum 10, got %d", got)
	}
}

func TestAddToCellNegativeDelta(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 10)); err != nil {
		t.Fatal(err)
	}
	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), -4)); err != nil {
		t.Fatal(err)
	}

	got, _ := readAggregate(t, s, table, row, "cf", []byte("hits"))
	if got != 6 {
		t.Fatalf("expected sum 6, got %d", got)
	}
}

// TestAddToCellConcurrent verifies that concurrent increments are atomic:
// Pebble merge operands commute, so no update is ever lost.
func TestAddToCellConcurrent(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("hot-counter")

	const workers = 8
	const addsPerWorker = 25

	var wg sync.WaitGroup
	errs := make(chan error, workers*addsPerWorker)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range addsPerWorker {
				if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 1)); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	got, _ := readAggregate(t, s, table, row, "cf", []byte("hits"))
	if got != workers*addsPerWorker {
		t.Fatalf("expected sum %d, got %d", workers*addsPerWorker, got)
	}
}

// TestAddToCellEndToEnd reads an aggregate back through ReadRows, verifying
// the merged value is exposed as an 8-byte big-endian int64 at timestamp 0.
func TestAddToCellEndToEnd(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 42)); err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows:      &bigtablepb.RowSet{RowKeys: [][]byte{row}},
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatal(err)
	}

	var chunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		chunks = append(chunks, resp.Chunks...)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if len(c.Value) != 8 {
		t.Fatalf("expected 8-byte value, got %d", len(c.Value))
	}
	if got := int64(binary.BigEndian.Uint64(c.Value)); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
	if c.TimestampMicros != 0 {
		t.Fatalf("aggregate cells should be timeless (ts=0), got %d", c.TimestampMicros)
	}
	if !c.GetCommitRow() {
		t.Fatal("expected commit_row on the aggregate chunk")
	}
}

// TestAddToCellRejectsNonAggregateCell verifies that adding to a cell holding
// a non-int64 SetCell value fails loudly instead of silently treating the
// existing value as zero.
func TestAddToCellRejectsNonAggregateCell(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("mixed-row")

	setMut := &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_SetCell_{
			SetCell: &bigtablepb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte("hits"),
				TimestampMicros: -1,
				Value:           []byte("abc"),
			},
		},
	}
	if err := mutateRow(t, s, table, row, setMut); err != nil {
		t.Fatal(err)
	}

	err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 1))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestAddToCellInvalidInput(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	badInput := &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_AddToCell_{
			AddToCell: &bigtablepb.Mutation_AddToCell{
				FamilyName: "cf",
				ColumnQualifier: &bigtablepb.Value{
					Kind: &bigtablepb.Value_RawValue{RawValue: []byte("hits")},
				},
				Input: &bigtablepb.Value{
					Kind: &bigtablepb.Value_StringValue{StringValue: "not-an-int"},
				},
			},
		},
	}
	if err := mutateRow(t, s, table, row, badInput); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}

	badQualifier := &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_AddToCell_{
			AddToCell: &bigtablepb.Mutation_AddToCell{
				FamilyName: "cf",
				ColumnQualifier: &bigtablepb.Value{
					Kind: &bigtablepb.Value_IntValue{IntValue: 5},
				},
				Input: &bigtablepb.Value{
					Kind: &bigtablepb.Value_IntValue{IntValue: 1},
				},
			},
		},
	}
	if err := mutateRow(t, s, table, row, badQualifier); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestMergeToCell(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	intInput := &bigtablepb.Value{Kind: &bigtablepb.Value_IntValue{IntValue: 15}}
	if err := mutateRow(t, s, table, row, mergeToCellMutation("cf", []byte("hits"), intInput)); err != nil {
		t.Fatal(err)
	}
	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 5)); err != nil {
		t.Fatal(err)
	}
	// Merging NULL is allowed and has no effect.
	if err := mutateRow(t, s, table, row, mergeToCellMutation("cf", []byte("hits"), nil)); err != nil {
		t.Fatal(err)
	}

	got, _ := readAggregate(t, s, table, row, "cf", []byte("hits"))
	if got != 20 {
		t.Fatalf("expected sum 20, got %d", got)
	}
}

func TestMutateRowsAddToCell(t *testing.T) {
	s := newTestServer(t)
	table := benchTable

	req := &bigtablepb.MutateRowsRequest{
		TableName: table,
		Entries: []*bigtablepb.MutateRowsRequest_Entry{
			{RowKey: []byte("row-a"), Mutations: []*bigtablepb.Mutation{addToCellMutation("cf", []byte("q"), 1)}},
			{RowKey: []byte("row-b"), Mutations: []*bigtablepb.Mutation{addToCellMutation("cf", []byte("q"), 2)}},
			{RowKey: []byte("row-a"), Mutations: []*bigtablepb.Mutation{addToCellMutation("cf", []byte("q"), 3)}},
		},
	}
	stream := newMockServerStream[*bigtablepb.MutateRowsResponse]()
	if err := s.MutateRows(req, stream); err != nil {
		t.Fatal(err)
	}
	for i, resp := range stream.sent {
		for _, e := range resp.Entries {
			if e.Status.GetCode() != 0 {
				t.Fatalf("resp %d entry %d failed: %v", i, e.Index, e.Status)
			}
		}
	}

	if got, _ := readAggregate(t, s, table, []byte("row-a"), "cf", []byte("q")); got != 4 {
		t.Fatalf("row-a: expected 4, got %d", got)
	}
	if got, _ := readAggregate(t, s, table, []byte("row-b"), "cf", []byte("q")); got != 2 {
		t.Fatalf("row-b: expected 2, got %d", got)
	}
}

// TestAddToCellAfterDelete verifies delete mutations clear the counter and
// subsequent adds start from zero again.
func TestAddToCellAfterDelete(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	row := []byte("counter-row")

	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 100)); err != nil {
		t.Fatal(err)
	}
	deleteMut := &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_DeleteFromRow_{
			DeleteFromRow: &bigtablepb.Mutation_DeleteFromRow{},
		},
	}
	if err := mutateRow(t, s, table, row, deleteMut); err != nil {
		t.Fatal(err)
	}
	if _, ok := readAggregate(t, s, table, row, "cf", []byte("hits")); ok {
		t.Fatal("expected counter to be deleted")
	}

	if err := mutateRow(t, s, table, row, addToCellMutation("cf", []byte("hits"), 5)); err != nil {
		t.Fatal(err)
	}
	if got, _ := readAggregate(t, s, table, row, "cf", []byte("hits")); got != 5 {
		t.Fatalf("expected 5 after delete+re-add, got %d", got)
	}
}
