package bigtable

import (
	"bytes"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// cellChunkBufferSize is the number of CellChunks to accumulate before sending.
const cellChunkBufferSize = 100

// maxCellChunkValueSize is the maximum value bytes in a single CellChunk.
// Values larger than this are split across multiple chunks with value_size hints.
const maxCellChunkValueSize = 64 * 1024 // 64 KB

// scanConfig controls the iteration direction.
type scanConfig struct {
	first func(*pebble.Iterator) bool
	next  func(*pebble.Iterator) bool
}

var forwardScan = scanConfig{
	first: (*pebble.Iterator).First,
	next:  (*pebble.Iterator).Next,
}

var reverseScan = scanConfig{
	first: (*pebble.Iterator).Last,
	next:  (*pebble.Iterator).Prev,
}

// ReadRows streams back the contents of all requested rows in key order.
func (s *Server) ReadRows(req *bigtablepb.ReadRowsRequest, stream grpc.ServerStreamingServer[bigtablepb.ReadRowsResponse]) error {
	eng, err := s.getEngine(stream.Context(), req.GetTableName())
	if err != nil {
		return status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()
	rowsLimit := req.GetRowsLimit()
	if rowsLimit == 0 {
		rowsLimit = 0 // unlimited
	}
	filter := req.GetFilter()
	rows := req.GetRows()

	cfg := forwardScan
	if req.GetReversed() {
		cfg = reverseScan
	}

	// Determine scan ranges.
	var scanRanges []pebble.KeyRange
	if rows == nil || (len(rows.GetRowKeys()) == 0 && len(rows.GetRowRanges()) == 0) {
		// Scan entire table.
		scanRanges = []pebble.KeyRange{{}}
	} else {
		for _, rk := range rows.GetRowKeys() {
			start, end := rowKeyRangeBounds(rk)
			scanRanges = append(scanRanges, pebble.KeyRange{Start: start, End: end})
		}
		for _, rr := range rows.GetRowRanges() {
			start, end := rowRangeToBounds(rr)
			scanRanges = append(scanRanges, pebble.KeyRange{Start: start, End: end})
		}
	}

	var filterEngine *rowFilterEngine
	if filter != nil {
		fe, err := newRowFilterEngine(filter)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
		}
		filterEngine = fe
	}

	// Emit CellChunks for each scan range.
	var chunkBuf []*bigtablepb.ReadRowsResponse_CellChunk
	var lastScannedRowKey []byte
	rowCount := int64(0)

	// commitLastChunk marks the last chunk in chunkBuf as commit_row.
	commitLastChunk := func() {
		if len(chunkBuf) == 0 {
			return
		}
		chunkBuf[len(chunkBuf)-1].RowStatus = &bigtablepb.ReadRowsResponse_CellChunk_CommitRow{
			CommitRow: true,
		}
	}

	// Flush sends buffered chunks to the stream.
	flush := func() error {
		if len(chunkBuf) == 0 {
			return nil
		}
		resp := &bigtablepb.ReadRowsResponse{
			Chunks: append([]*bigtablepb.ReadRowsResponse_CellChunk(nil), chunkBuf...),
		}
		if len(lastScannedRowKey) > 0 {
			resp.LastScannedRowKey = append([]byte(nil), lastScannedRowKey...)
		}
		chunkBuf = chunkBuf[:0]
		return stream.Send(resp)
	}

	for _, kr := range scanRanges {
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: kr.Start,
			UpperBound: kr.End,
		})
		if err != nil {
			continue
		}
		cfg.first(iter)

		var lastRowKey []byte
		var rowStarted bool

		for ; iter.Valid(); cfg.next(iter) {
			rk, family, qualifier, ts, ok := DecodeCellKey(iter.Key())
			if !ok {
				continue
			}

			// Check row boundary.
			if !bytes.Equal(rk, lastRowKey) {
				if rowStarted {
					// Close the previous row.
					commitLastChunk()
					lastScannedRowKey = append([]byte(nil), lastRowKey...)
				}
				rowCount++
				if rowsLimit > 0 && rowCount > rowsLimit {
					break
				}
				// Reset per-row filter state for the new row.
				if filterEngine != nil {
					filterEngine.eval.reset()
				}
				lastRowKey = append(lastRowKey[:0], rk...)
				rowStarted = true
			}

			val := iter.Value()
			if len(val) > 0 {
				val = append([]byte(nil), val...)
			}

			// Apply filter.
			if filterEngine != nil && !filterEngine.matchesCell(rk, family, qualifier, ts, val) {
				continue
			}

			// Strip value if the filter chain includes a strip-value transformer.
			if filterEngine != nil && filterEngine.hasStripValue() {
				val = nil
			}

			// Emit cell chunk(s) — split large values across multiple chunks.
			chunkBuf = appendCellChunks(chunkBuf, rk, family, qualifier, ts, val)

			if len(chunkBuf) >= cellChunkBufferSize {
				if err := flush(); err != nil {
					_ = iter.Close()
					return err
				}
			}
		}

		if rowStarted {
			commitLastChunk()
			lastScannedRowKey = append([]byte(nil), lastRowKey...)
		}
		_ = iter.Close()

		if rowsLimit > 0 && rowCount >= rowsLimit {
			break
		}
	}

	return flush()
}

// rowRangeToBounds converts a Bigtable RowRange to Pebble scan bounds.
func rowRangeToBounds(rr *bigtablepb.RowRange) (start, end []byte) {
	switch s := rr.StartKey.(type) {
	case *bigtablepb.RowRange_StartKeyClosed:
		start = encodeRowPrefix(s.StartKeyClosed)
	case *bigtablepb.RowRange_StartKeyOpen:
		start = encodeRowPrefix(s.StartKeyOpen)
		start = append(start, 0xFF) // after any cells starting with this key
	default:
		start = nil // beginning of table
	}

	switch e := rr.EndKey.(type) {
	case *bigtablepb.RowRange_EndKeyClosed:
		end = encodeRowPrefix(e.EndKeyClosed)
		end = rowEndKey(end)
	case *bigtablepb.RowRange_EndKeyOpen:
		end = encodeRowPrefix(e.EndKeyOpen)
	default:
		end = nil // end of table
	}

	return start, end
}

// appendCellChunks appends one or more CellChunks for a cell value.
// Values larger than maxCellChunkValueSize are split across multiple chunks
// with value_size hints (total size) on all but the last chunk.
// Only the first chunk carries the full cell metadata (row_key, family,
// qualifier, timestamp). Continuation chunks only carry value and value_size.
func appendCellChunks(buf []*bigtablepb.ReadRowsResponse_CellChunk, rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte) []*bigtablepb.ReadRowsResponse_CellChunk {
	if len(value) <= maxCellChunkValueSize {
		return append(buf, cellChunk(rowKey, family, qualifier, timestampMicros, value, nil))
	}
	totalSize := len(value)
	for offset := 0; offset < totalSize; offset += maxCellChunkValueSize {
		end := min(offset+maxCellChunkValueSize, totalSize)
		var chunk *bigtablepb.ReadRowsResponse_CellChunk
		if offset == 0 {
			chunk = cellChunk(rowKey, family, qualifier, timestampMicros, value[offset:end], nil)
		} else {
			// Continuation chunks carry only value (and optional value_size).
			chunk = &bigtablepb.ReadRowsResponse_CellChunk{
				Value: value[offset:end],
			}
		}
		if end < totalSize {
			if totalSize > math.MaxInt32 {
				chunk.ValueSize = math.MaxInt32
			} else {
				chunk.ValueSize = int32(totalSize) //nolint:gosec
			}
		}
		buf = append(buf, chunk)
	}
	return buf
}

// cellChunk creates a CellChunk for a single cell with full metadata.
// rowKey is only set for the first cell of each row (caller should track this).
func cellChunk(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) *bigtablepb.ReadRowsResponse_CellChunk {
	chunk := &bigtablepb.ReadRowsResponse_CellChunk{
		TimestampMicros: timestampMicros,
		Labels:          labels,
	}
	if len(value) > 0 {
		chunk.Value = make([]byte, len(value))
		copy(chunk.Value, value)
	}
	if len(rowKey) > 0 {
		chunk.RowKey = make([]byte, len(rowKey))
		copy(chunk.RowKey, rowKey)
	}
	if family != "" {
		chunk.FamilyName = wrapperspb.String(family)
	}
	if len(qualifier) > 0 {
		chunk.Qualifier = wrapperspb.Bytes(qualifier)
	}
	return chunk
}

// commitRowChunk creates a CellChunk that marks the end of a row.
func commitRowChunk() *bigtablepb.ReadRowsResponse_CellChunk {
	return &bigtablepb.ReadRowsResponse_CellChunk{
		RowStatus: &bigtablepb.ReadRowsResponse_CellChunk_CommitRow{
			CommitRow: true,
		},
	}
}

// resetRowChunk creates a CellChunk that tells the client to discard the
// current row being accumulated (error recovery sentinel).
func resetRowChunk() *bigtablepb.ReadRowsResponse_CellChunk {
	return &bigtablepb.ReadRowsResponse_CellChunk{
		RowStatus: &bigtablepb.ReadRowsResponse_CellChunk_ResetRow{
			ResetRow: true,
		},
	}
}

// rowTerminal returns true if the chunk is a commit_row or reset_row sentinel.
func rowTerminal(chunk *bigtablepb.ReadRowsResponse_CellChunk) bool {
	return chunk.GetCommitRow() || chunk.GetResetRow()
}
