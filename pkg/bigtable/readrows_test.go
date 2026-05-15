package bigtable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// populateTable writes a known set of cells for ReadRows tests.
func populateTable(t *testing.T, s *Server, table string) {
	t.Helper()
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	data := []struct {
		row  string
		fam  string
		qual string
		ts   int64
		val  string
	}{
		{"row1", "cf1", "a", 100, "v1"},
		{"row1", "cf1", "b", 200, "v2"},
		{"row1", "cf2", "c", 300, "v3"},
		{"row2", "cf1", "a", 100, "v4"},
		{"row3", "cf1", "a", 100, "v5"},
	}
	for _, d := range data {
		key := EncodeCellKey([]byte(d.row), d.fam, []byte(d.qual), d.ts)
		if err := db.Set(key, []byte(d.val), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
}

// TestReadRowsValuesSurviveIteratorMove verifies that cell values returned
// by ReadRows are correctly copied and not corrupted by Pebble's iterator
// buffer reuse. The bug: cellChunk stored iter.Value() as a direct slice
// reference, which is only valid until the next iterator operation. After
// Flush forces data into SSTs, Pebble reuses block buffers across iterator
// moves, causing previously-returned values to be silently overwritten.
func TestReadRowsValuesSurviveIteratorMove(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	// Write enough cells across rows to span multiple SST blocks.
	for i := range 100 {
		row := fmt.Sprintf("row-%03d", i)
		for j := range 5 {
			key := EncodeCellKey([]byte(row), "cf", []byte{byte(j)}, 1000+int64(j))
			val := fmt.Sprintf("val-%03d-%d", i, j)
			if err := db.Set(key, []byte(val), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
	}

	// ReadRows must return all 500 cells with correct values.
	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
	}
	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	if err := s.ReadRows(req, stream); err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	got := make(map[string]int)
	for _, c := range allChunks {
		if c.GetResetRow() {
			continue
		}
		if len(c.Value) == 0 && c.ValueSize == 0 {
			continue
		}
		got[string(c.Value)]++
	}

	for i := range 100 {
		for j := range 5 {
			want := fmt.Sprintf("val-%03d-%d", i, j)
			if got[want] != 1 {
				t.Fatalf("value %q: got count %d, want 1; total unique=%d, chunks=%d", want, got[want], len(got), len(allChunks))
			}
		}
	}
	if len(got) != 500 {
		t.Fatalf("expected 500 unique values, got %d", len(got))
	}
}

func TestReadRowsSingleKey(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowKeys: [][]byte{[]byte("row1")},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get one response with chunks for row1's cells.
	if len(stream.sent) == 0 {
		t.Fatal("expected at least one response")
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Expect 3 chunks (3 cells, last with commit_row).
	if len(allChunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(allChunks))
	}

	// First chunk should have row_key set.
	if len(allChunks[0].RowKey) == 0 {
		t.Fatal("first chunk should have row_key set")
	}
	if !bytes.Equal(allChunks[0].RowKey, []byte("row1")) {
		t.Fatalf("row key mismatch: got %q", allChunks[0].RowKey)
	}

	// Last chunk should have commit_row.
	if !allChunks[len(allChunks)-1].GetCommitRow() {
		t.Fatal("last chunk should have commit_row")
	}
}

func TestReadRowsMultipleKeys(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowKeys: [][]byte{[]byte("row1"), []byte("row3")},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Count commit row markers.
	rowCount := 0
	for _, c := range allChunks {
		if c.GetCommitRow() {
			rowCount++
		}
	}
	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}
}

func TestReadRowsRowRange(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowRanges: []*bigtablepb.RowRange{
				{
					StartKey: &bigtablepb.RowRange_StartKeyClosed{
						StartKeyClosed: []byte("row1"),
					},
					EndKey: &bigtablepb.RowRange_EndKeyOpen{
						EndKeyOpen: []byte("row3"),
					},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}

	if !rowKeys["row1"] {
		t.Fatal("expected row1 in results")
	}
	if !rowKeys["row2"] {
		t.Fatal("expected row2 in results")
	}
	if rowKeys["row3"] {
		t.Fatal("expected row3 to be excluded (end key open)")
	}
}

func TestReadRowsFullScan(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Should get all 3 rows.
	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}

	if len(rowKeys) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rowKeys))
	}
}

func TestReadRowsWithFilter(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowKeys: [][]byte{[]byte("row1")},
		},
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_FamilyNameRegexFilter{
				FamilyNameRegexFilter: "cf1",
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Should get 2 chunks (cf1:a and cf1:b, last with commit_row).
	if len(allChunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(allChunks))
	}

	for _, c := range allChunks {
		fam := c.GetFamilyName().GetValue()
		if fam != "cf1" {
			t.Fatalf("expected family cf1, got %q", fam)
		}
	}
}

func TestReadRowsBlockAllFilter(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowKeys: [][]byte{[]byte("row1")},
		},
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_BlockAllFilter{},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All data cells filtered — no rows should be emitted.
	if len(stream.sent) != 0 {
		t.Fatalf("expected no responses, got %d", len(stream.sent))
	}
}

func TestReadRowsRowsLimit(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		RowsLimit: 1,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}
	if len(rowKeys) != 1 || !rowKeys["row1"] {
		t.Fatalf("expected only row1, got %v", rowKeys)
	}
}

func TestReadRowsEmptyTable(t *testing.T) {
	s := newTestServer(t)
	table := benchTable

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stream.sent) > 0 {
		t.Fatal("expected no responses for empty table")
	}
}

func TestReadRowsNonExistentKey(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowKeys: [][]byte{[]byte("nonexistent")},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stream.sent) > 0 {
		t.Fatal("expected no responses for nonexistent key")
	}
}

func TestReadRowsOpenStartRange(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowRanges: []*bigtablepb.RowRange{
				{
					StartKey: &bigtablepb.RowRange_StartKeyOpen{
						StartKeyOpen: []byte("row1"),
					},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}

	if rowKeys["row1"] {
		t.Fatal("expected row1 to be excluded (open start)")
	}
	if !rowKeys["row2"] {
		t.Fatal("expected row2 in results")
	}
	if !rowKeys["row3"] {
		t.Fatal("expected row3 in results")
	}
}

func TestReadRowsEndClosedRange(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowRanges: []*bigtablepb.RowRange{
				{
					EndKey: &bigtablepb.RowRange_EndKeyClosed{
						EndKeyClosed: []byte("row2"),
					},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}

	if !rowKeys["row1"] {
		t.Fatal("expected row1 in results")
	}
	if !rowKeys["row2"] {
		t.Fatal("expected row2 in results (closed end)")
	}
	if rowKeys["row3"] {
		t.Fatal("expected row3 to be excluded")
	}
}

func TestReadRowsWithMultipleScenarios(t *testing.T) {
	s := newTestServer(t)
	table := benchTable

	// Write many cells across rows to trigger chunk buffering.
	eng := openTableEngine(t, s, table)
	db := eng.DB()
	for i := range 150 {
		key := EncodeCellKey([]byte("row"), "cf", []byte("q"), int64(i))
		if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have multiple responses due to buffering.
	if len(stream.sent) < 2 {
		t.Fatalf("expected multiple responses due to chunk buffering, got %d", len(stream.sent))
	}
}

func TestCellChunk(t *testing.T) {
	chunk := cellChunk([]byte("row1"), "cf", []byte("q"), 1000000, []byte("val"), nil)
	if !bytes.Equal(chunk.RowKey, []byte("row1")) {
		t.Fatalf("row key: got %v", chunk.RowKey)
	}
	if chunk.TimestampMicros != 1000000 {
		t.Fatalf("timestamp: got %d", chunk.TimestampMicros)
	}
	if !bytes.Equal(chunk.Value, []byte("val")) {
		t.Fatalf("value: got %v", chunk.Value)
	}
	if chunk.GetFamilyName().GetValue() != "cf" {
		t.Fatalf("family: got %q", chunk.GetFamilyName().GetValue())
	}
	if !bytes.Equal(chunk.GetQualifier().GetValue(), []byte("q")) {
		t.Fatalf("qualifier: got %v", chunk.GetQualifier().GetValue())
	}
}

func TestCellChunkWithRowKeyOnly(t *testing.T) {
	chunk := cellChunk([]byte("row1"), "", nil, 0, nil, nil)
	if !bytes.Equal(chunk.RowKey, []byte("row1")) {
		t.Fatalf("row key: got %v", chunk.RowKey)
	}
	// When family is empty, FamilyName should not be set.
	if chunk.FamilyName != nil {
		t.Fatal("expected nil FamilyName when family is empty")
	}
	// When qualifier is nil, Qualifier should not be set.
	if chunk.Qualifier != nil {
		t.Fatal("expected nil Qualifier when qualifier is nil")
	}
}

func TestCellChunkLabels(t *testing.T) {
	labels := []string{"label1", "label2"}
	chunk := cellChunk([]byte("row1"), "cf", []byte("q"), 100, []byte("val"), labels)
	if len(chunk.Labels) != 2 {
		t.Fatalf("expected 2 labels, got %d", len(chunk.Labels))
	}
	if chunk.Labels[0] != "label1" {
		t.Fatalf("label[0]: got %q", chunk.Labels[0])
	}
}

func TestCommitRowChunk(t *testing.T) {
	chunk := commitRowChunk()
	if !chunk.GetCommitRow() {
		t.Fatal("expected CommitRow to be true")
	}
}

func TestAppendCellChunksSmallValue(t *testing.T) {
	buf := appendCellChunks(nil, []byte("row1"), "cf", []byte("q"), 100, []byte("small"))
	if len(buf) != 1 {
		t.Fatalf("expected 1 chunk for small value, got %d", len(buf))
	}
	if string(buf[0].Value) != "small" {
		t.Fatalf("value mismatch: got %q", buf[0].Value)
	}
	if buf[0].ValueSize != 0 {
		t.Fatal("value_size should be 0 for single-chunk values")
	}
}

func TestAppendCellChunksLargeValue(t *testing.T) {
	largeVal := make([]byte, maxCellChunkValueSize+1)
	for i := range largeVal {
		largeVal[i] = byte(i)
	}
	buf := appendCellChunks(nil, []byte("row1"), "cf", []byte("q"), 100, largeVal)
	if len(buf) != 2 {
		t.Fatalf("expected 2 chunks for %d-byte value, got %d", len(largeVal), len(buf))
	}
	// First chunk should have value_size hint set to total length.
	if int(buf[0].ValueSize) != len(largeVal) {
		t.Fatalf("expected value_size=%d on first chunk, got %d", len(largeVal), buf[0].ValueSize)
	}
	if len(buf[0].Value) != maxCellChunkValueSize {
		t.Fatalf("first chunk should be %d bytes, got %d", maxCellChunkValueSize, len(buf[0].Value))
	}
	if buf[1].ValueSize != 0 {
		t.Fatal("last chunk should not have value_size set")
	}
	if len(buf[1].Value) != 1 {
		t.Fatalf("last chunk should be 1 byte, got %d", len(buf[1].Value))
	}
	// Verify value reconstruction.
	reconstructed := append([]byte(nil), buf[0].Value...)
	reconstructed = append(reconstructed, buf[1].Value...)
	if !bytes.Equal(reconstructed, largeVal) {
		t.Fatal("reconstructed value should match original")
	}
}

func TestResetRowChunk(t *testing.T) {
	chunk := resetRowChunk()
	if !chunk.GetResetRow() {
		t.Fatal("expected ResetRow to be true")
	}
}

func TestRowTerminal(t *testing.T) {
	if !rowTerminal(commitRowChunk()) {
		t.Fatal("commitRowChunk should be terminal")
	}
	if !rowTerminal(resetRowChunk()) {
		t.Fatal("resetRowChunk should be terminal")
	}
	if rowTerminal(&bigtablepb.ReadRowsResponse_CellChunk{Value: []byte("data")}) {
		t.Fatal("data chunk should not be terminal")
	}
}

func TestRowRangeToBounds(t *testing.T) {
	t.Run("start closed end open", func(t *testing.T) {
		rr := &bigtablepb.RowRange{
			StartKey: &bigtablepb.RowRange_StartKeyClosed{
				StartKeyClosed: []byte("row1"),
			},
			EndKey: &bigtablepb.RowRange_EndKeyOpen{
				EndKeyOpen: []byte("row3"),
			},
		}
		start, end := rowRangeToBounds(rr)
		// start should be row1 prefix.
		expectedStart := encodeRowPrefix([]byte("row1"))
		if !bytes.Equal(start, expectedStart) {
			t.Fatalf("start: got %v, want %v", start, expectedStart)
		}
		// end should be row3 prefix (open = exclusive).
		expectedEnd := encodeRowPrefix([]byte("row3"))
		if !bytes.Equal(end, expectedEnd) {
			t.Fatalf("end: got %v, want %v", end, expectedEnd)
		}
	})

	t.Run("start open end closed", func(t *testing.T) {
		rr := &bigtablepb.RowRange{
			StartKey: &bigtablepb.RowRange_StartKeyOpen{
				StartKeyOpen: []byte("row1"),
			},
			EndKey: &bigtablepb.RowRange_EndKeyClosed{
				EndKeyClosed: []byte("row3"),
			},
		}
		start, _ := rowRangeToBounds(rr)
		expectedStart := encodeRowPrefix([]byte("row1"))
		expectedStart = append(expectedStart, 0xFF)
		if !bytes.Equal(start, expectedStart) {
			t.Fatalf("start (open): got %v, want %v", start, expectedStart)
		}
	})

	t.Run("no bounds", func(t *testing.T) {
		rr := &bigtablepb.RowRange{}
		start, end := rowRangeToBounds(rr)
		if start != nil {
			t.Fatal("expected nil start")
		}
		if end != nil {
			t.Fatal("expected nil end")
		}
	})
}

func TestReadRowsInvalidFilter(t *testing.T) {
	s := newTestServer(t)
	table := benchTable

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Filter: &bigtablepb.RowFilter{
			Filter: &bigtablepb.RowFilter_RowKeyRegexFilter{
				RowKeyRegexFilter: []byte("[invalid"),
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestReadRowsReversed(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Reversed:  true,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	// Build ordered unique row keys from chunks (RowKey set on first cell of each row).
	seen := make(map[string]bool)
	var rowKeysInOrder [][]byte
	for _, c := range allChunks {
		key := string(c.RowKey)
		if len(c.RowKey) > 0 && !seen[key] {
			seen[key] = true
			rowKeysInOrder = append(rowKeysInOrder, c.RowKey)
		}
	}

	// Expect 3 rows in reverse order: row3, row2, row1.
	if len(rowKeysInOrder) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rowKeysInOrder))
	}
	if string(rowKeysInOrder[0]) != "row3" {
		t.Fatalf("expected first row to be row3 (reversed), got %q", rowKeysInOrder[0])
	}
	if string(rowKeysInOrder[1]) != "row2" {
		t.Fatalf("expected second row to be row2, got %q", rowKeysInOrder[1])
	}
	if string(rowKeysInOrder[2]) != "row1" {
		t.Fatalf("expected third row to be row1, got %q", rowKeysInOrder[2])
	}
}

func TestReadRowsReversedWithLimit(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Reversed:  true,
		RowsLimit: 1,
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	seen := make(map[string]bool)
	var rowKeysInOrder [][]byte
	for _, c := range allChunks {
		key := string(c.RowKey)
		if len(c.RowKey) > 0 && !seen[key] {
			seen[key] = true
			rowKeysInOrder = append(rowKeysInOrder, c.RowKey)
		}
	}

	if len(rowKeysInOrder) != 1 || string(rowKeysInOrder[0]) != "row3" {
		t.Fatalf("expected only row3 (last row, reversed with limit 1), got %v", rowKeysInOrder)
	}
}

func TestReadRowsPartialResponseMultipleRanges(t *testing.T) {
	s := newTestServer(t)
	table := benchTable
	populateTable(t, s, table)

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
		Rows: &bigtablepb.RowSet{
			RowRanges: []*bigtablepb.RowRange{
				{
					StartKey: &bigtablepb.RowRange_StartKeyClosed{StartKeyClosed: []byte("row1")},
					EndKey:   &bigtablepb.RowRange_EndKeyOpen{EndKeyOpen: []byte("row2")},
				},
				{
					StartKey: &bigtablepb.RowRange_StartKeyClosed{StartKeyClosed: []byte("row3")},
				},
			},
		},
	}

	stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
	err := s.ReadRows(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var allChunks []*bigtablepb.ReadRowsResponse_CellChunk
	for _, resp := range stream.sent {
		allChunks = append(allChunks, resp.Chunks...)
	}

	rowKeys := make(map[string]bool)
	for _, c := range allChunks {
		if len(c.RowKey) > 0 {
			rowKeys[string(c.RowKey)] = true
		}
	}

	if !rowKeys["row1"] {
		t.Fatal("expected row1")
	}
	if rowKeys["row2"] {
		t.Fatal("expected row2 excluded from first range")
	}
	if !rowKeys["row3"] {
		t.Fatal("expected row3")
	}
}
