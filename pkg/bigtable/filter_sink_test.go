package bigtable

import (
	"testing"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

// readAllRows runs a full-table ReadRows with the given filter and returns
// the flattened chunk list.
func readAllRows(t *testing.T, s *Server, table string, filter *bigtablepb.RowFilter) []*bigtablepb.ReadRowsResponse_CellChunk {
	t.Helper()
	req := &bigtablepb.ReadRowsRequest{TableName: table, Filter: filter}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var chunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		chunks = append(chunks, resp.Chunks...)
	}
	return chunks
}

// TestSinkFilterTeesCells verifies the sink semantics: chain{sink, blockAll}
// emits every scanned cell even though the chain's final verdict rejects all.
func TestSinkFilterTeesCells(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	blockAll := &bigtablepb.RowFilter{Filter: &bigtablepb.RowFilter_BlockAllFilter{}}
	if chunks := readAllRows(t, s, table, blockAll); len(chunks) != 0 {
		t.Fatalf("blockAll alone: expected 0 chunks, got %d", len(chunks))
	}

	sinkThenBlock := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Chain_{
			Chain: &bigtablepb.RowFilter_Chain{
				Filters: []*bigtablepb.RowFilter{
					{Filter: &bigtablepb.RowFilter_Sink{Sink: true}},
					blockAll,
				},
			},
		},
	}
	chunks := readAllRows(t, s, table, sinkThenBlock)
	// populateTable writes 5 cells; every one must be teed out by the sink.
	if len(chunks) != 5 {
		t.Fatalf("sink: expected 5 chunks, got %d", len(chunks))
	}
}

// TestSinkFilterAfterSelectiveStage verifies that in chain{familyRegex,
// sink, blockAll}, only cells passing the stages before the sink are teed
// out: the sink marks exactly the cells that reach it.
func TestSinkFilterAfterSelectiveStage(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	filter := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Chain_{
			Chain: &bigtablepb.RowFilter_Chain{
				Filters: []*bigtablepb.RowFilter{
					{Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "cf1"}},
					{Filter: &bigtablepb.RowFilter_Sink{Sink: true}},
					{Filter: &bigtablepb.RowFilter_BlockAllFilter{}},
				},
			},
		},
	}
	chunks := readAllRows(t, s, table, filter)
	// cf1 cells: row1:a, row1:b, row2:a, row3:a. The cf2 cell never reaches
	// the sink and is not emitted.
	if len(chunks) != 4 {
		t.Fatalf("expected 4 sunk chunks, got %d", len(chunks))
	}

	// Decode family per continuation semantics and verify all are cf1.
	fam := ""
	for _, c := range chunks {
		if c.FamilyName != nil {
			fam = c.GetFamilyName().GetValue()
		}
		if fam != "cf1" {
			t.Fatalf("expected only cf1 cells, got family %q", fam)
		}
	}
}

// TestSinkFilterDoesNotDuplicate verifies a cell passing both the sink and
// the final filter is emitted exactly once.
func TestSinkFilterDoesNotDuplicate(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	filter := &bigtablepb.RowFilter{
		Filter: &bigtablepb.RowFilter_Chain_{
			Chain: &bigtablepb.RowFilter_Chain{
				Filters: []*bigtablepb.RowFilter{
					{Filter: &bigtablepb.RowFilter_Sink{Sink: true}},
					{Filter: &bigtablepb.RowFilter_PassAllFilter{PassAllFilter: true}},
				},
			},
		},
	}
	if chunks := readAllRows(t, s, table, filter); len(chunks) != 5 {
		t.Fatalf("expected 5 chunks (no duplicates), got %d", len(chunks))
	}
}
