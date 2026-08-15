package bigtable

import (
	"bytes"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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
	startTime := time.Now()
	statsView := req.GetRequestStatsView()
	var rowsSeen, rowsReturned, cellsSeen, cellsReturned int64
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
	chunkBuf := make([]*bigtablepb.ReadRowsResponse_CellChunk, 0, cellChunkBufferSize)
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

	// Flush sends buffered chunks to the stream. Ownership of chunkBuf
	// transfers to the response; a fresh buffer is allocated for the next
	// batch to avoid mutating a message already handed to the stream.
	flush := func() error {
		if len(chunkBuf) == 0 {
			return nil
		}
		resp := &bigtablepb.ReadRowsResponse{
			Chunks: chunkBuf,
		}
		if len(lastScannedRowKey) > 0 {
			resp.LastScannedRowKey = append([]byte(nil), lastScannedRowKey...)
		}
		chunkBuf = make([]*bigtablepb.ReadRowsResponse_CellChunk, 0, cellChunkBufferSize)
		return stream.Send(resp)
	}

	var dec CellDecoder

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

		// Column coordinates of the last chunk sent on the stream. Per the
		// ReadRows protocol, row_key/family_name/qualifier are only sent when
		// they change; omitted fields continue the previous chunk's values.
		// This matches real Bigtable behavior and avoids allocating wrapper
		// messages and key copies for every cell.
		var sentFamily string
		var sentQualifier []byte
		// rowCoordsPending marks that the next emitted cell starts a new row
		// and must carry full coordinates. Tracked separately from the scan
		// position because filters may skip a row's leading cells.
		rowCoordsPending := false

		for ; iter.Valid(); cfg.next(iter) {
			rk, family, qualifier, ts, ok := dec.Decode(iter.Key())
			if !ok {
				continue
			}
			cellsSeen++

			// Check row boundary.
			if !bytes.Equal(rk, lastRowKey) {
				if rowStarted {
					commitLastChunk()
					lastScannedRowKey = append(lastScannedRowKey[:0], lastRowKey...)
				}
				rowCount++
				if rowsLimit > 0 && rowCount > rowsLimit {
					break
				}
				if filterEngine != nil {
					filterEngine.eval.reset()
				}
				lastRowKey = append(lastRowKey[:0], rk...)
				rowStarted = true
				rowCoordsPending = true
				rowsSeen++
			}

			val := iter.Value()

			if filterEngine != nil && !filterEngine.matchesCell(rk, family, qualifier, ts, val) {
				continue
			}

			if filterEngine != nil && filterEngine.hasStripValue() {
				val = nil
			}

			// Send cell coordinates only when they change. The first emitted
			// cell of a row always carries the full coordinates so clients
			// never associate it with a column from the previous row.
			var chunkRowKey []byte
			chunkFamily := ""
			var chunkQualifier []byte
			if rowCoordsPending {
				chunkRowKey = rk
				chunkFamily = family
				chunkQualifier = qualifier
				rowCoordsPending = false
				rowsReturned++
			} else {
				if family != sentFamily {
					chunkFamily = family
				}
				if !bytes.Equal(qualifier, sentQualifier) {
					chunkQualifier = qualifier
				}
			}

			chunkBuf = appendCellChunks(chunkBuf, chunkRowKey, chunkFamily, chunkQualifier, ts, val)
			cellsReturned++
			sentFamily = family
			sentQualifier = append(sentQualifier[:0], qualifier...)

			if len(chunkBuf) >= cellChunkBufferSize {
				if err := flush(); err != nil {
					_ = iter.Close()
					return err
				}
			}
		}

		if rowStarted {
			commitLastChunk()
			lastScannedRowKey = append(lastScannedRowKey[:0], lastRowKey...)
		}
		_ = iter.Close()

		if rowsLimit > 0 && rowCount >= rowsLimit {
			break
		}
	}

	if statsView == bigtablepb.ReadRowsRequest_REQUEST_STATS_FULL {
		// RequestStats is attached to the last message of the stream. gRPC
		// marshals synchronously at Send time, so it must be set before the
		// final flush rather than retrofitted onto an earlier response.
		resp := &bigtablepb.ReadRowsResponse{
			Chunks: chunkBuf,
			RequestStats: &bigtablepb.RequestStats{
				StatsView: &bigtablepb.RequestStats_FullReadStatsView{
					FullReadStatsView: &bigtablepb.FullReadStatsView{
						ReadIterationStats: &bigtablepb.ReadIterationStats{
							RowsSeenCount:      rowsSeen,
							RowsReturnedCount:   rowsReturned,
							CellsSeenCount:      cellsSeen,
							CellsReturnedCount:   cellsReturned,
						},
						RequestLatencyStats: &bigtablepb.RequestLatencyStats{
							FrontendServerLatency: durationpb.New(time.Since(startTime)),
						},
					},
				},
			},
		}
		if len(lastScannedRowKey) > 0 {
			resp.LastScannedRowKey = append([]byte(nil), lastScannedRowKey...)
		}
		// Send even when no chunks remain so the stats are delivered.
		return stream.Send(resp)
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
			// Copy the slice to avoid referencing the iterator's internal buffer.
			cv := make([]byte, end-offset)
			copy(cv, value[offset:end])
			chunk = &bigtablepb.ReadRowsResponse_CellChunk{
				Value: cv,
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
// All byte-slice fields are copied out of iterator-owned memory in a single
// shared allocation to minimize per-cell allocation count.
func cellChunk(rowKey []byte, family string, qualifier []byte, timestampMicros int64, value []byte, labels []string) *bigtablepb.ReadRowsResponse_CellChunk {
	chunk := &bigtablepb.ReadRowsResponse_CellChunk{
		TimestampMicros: timestampMicros,
		Labels:          labels,
	}
	if n := len(rowKey) + len(qualifier) + len(value); n > 0 {
		buf := make([]byte, n)
		pos := 0
		if len(rowKey) > 0 {
			chunk.RowKey = buf[:len(rowKey)]
			copy(chunk.RowKey, rowKey)
			pos += len(rowKey)
		}
		if len(qualifier) > 0 {
			chunk.Qualifier = wrapperspb.Bytes(buf[pos : pos+len(qualifier)])
			copy(buf[pos:], qualifier)
			pos += len(qualifier)
		}
		if len(value) > 0 {
			chunk.Value = buf[pos:]
			copy(chunk.Value, value)
		}
	}
	if family != "" {
		chunk.FamilyName = wrapperspb.String(family)
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
