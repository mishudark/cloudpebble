package bigtable

import (
	"testing"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

// TestReadRowsRequestStatsFull verifies that REQUEST_STATS_FULL attaches
// RequestStats with iteration counts and latency to the last response.
func TestReadRowsRequestStatsFull(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName:        table,
		RequestStatsView: bigtablepb.ReadRowsRequest_REQUEST_STATS_FULL,
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) == 0 {
		t.Fatal("expected at least one response")
	}

	// Only the last message carries stats.
	last := stream.sent[len(stream.sent)-1]
	for _, resp := range stream.sent[:len(stream.sent)-1] {
		if resp.RequestStats != nil {
			t.Fatal("RequestStats set on a non-final response")
		}
	}

	stats := last.GetRequestStats().GetFullReadStatsView()
	if stats == nil {
		t.Fatal("expected FullReadStatsView on the last response")
	}
	iter := stats.GetReadIterationStats()
	if iter.GetRowsSeenCount() != 3 {
		t.Fatalf("rows seen: expected 3, got %d", iter.GetRowsSeenCount())
	}
	if iter.GetRowsReturnedCount() != 3 {
		t.Fatalf("rows returned: expected 3, got %d", iter.GetRowsReturnedCount())
	}
	if iter.GetCellsSeenCount() != 5 {
		t.Fatalf("cells seen: expected 5, got %d", iter.GetCellsSeenCount())
	}
	if iter.GetCellsReturnedCount() != 5 {
		t.Fatalf("cells returned: expected 5, got %d", iter.GetCellsReturnedCount())
	}
	if stats.GetRequestLatencyStats().GetFrontendServerLatency() == nil {
		t.Fatal("expected frontend server latency to be set")
	}
}

// TestReadRowsRequestStatsWithFilter verifies seen/returned counts diverge
// when a filter drops cells.
func TestReadRowsRequestStatsWithFilter(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "cf1"},
		},
		RequestStatsView: bigtablepb.ReadRowsRequest_REQUEST_STATS_FULL,
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatal(err)
	}

	last := stream.sent[len(stream.sent)-1]
	iter := last.GetRequestStats().GetFullReadStatsView().GetReadIterationStats()
	if iter.GetCellsSeenCount() != 5 {
		t.Fatalf("cells seen: expected 5, got %d", iter.GetCellsSeenCount())
	}
	if iter.GetCellsReturnedCount() != 4 {
		t.Fatalf("cells returned: expected 4, got %d", iter.GetCellsReturnedCount())
	}
	if iter.GetRowsSeenCount() != 3 || iter.GetRowsReturnedCount() != 3 {
		t.Fatalf("rows: expected 3/3, got %d/%d", iter.GetRowsSeenCount(), iter.GetRowsReturnedCount())
	}
}

// TestReadRowsRequestStatsEmptyResult verifies stats are still delivered
// (in an otherwise empty response) when nothing matches.
func TestReadRowsRequestStatsEmptyResult(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_BlockAllFilter{},
		},
		RequestStatsView: bigtablepb.ReadRowsRequest_REQUEST_STATS_FULL,
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expected exactly one stats-only response, got %d", len(stream.sent))
	}
	resp := stream.sent[0]
	if len(resp.Chunks) != 0 {
		t.Fatalf("expected no chunks, got %d", len(resp.Chunks))
	}
	iter := resp.GetRequestStats().GetFullReadStatsView().GetReadIterationStats()
	if iter.GetCellsSeenCount() != 5 || iter.GetCellsReturnedCount() != 0 {
		t.Fatalf("expected 5 seen / 0 returned, got %d/%d", iter.GetCellsSeenCount(), iter.GetCellsReturnedCount())
	}
}

// TestReadRowsRequestStatsNone verifies no stats are attached by default.
func TestReadRowsRequestStatsNone(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{TableName: table}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatal(err)
	}
	for _, resp := range stream.sent {
		if resp.RequestStats != nil {
			t.Fatal("RequestStats set without REQUEST_STATS_FULL")
		}
	}
}
