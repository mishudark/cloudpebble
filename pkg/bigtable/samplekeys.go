package bigtable

import (
	"math"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SampleRowKeys returns a sample of row keys in the table, delimited by
// approximate SST file boundaries.
func (s *Server) SampleRowKeys(req *bigtablepb.SampleRowKeysRequest, stream grpc.ServerStreamingServer[bigtablepb.SampleRowKeysResponse]) error {
	eng, err := s.getEngine(stream.Context(), req.GetTableName())
	if err != nil {
		return status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()

	// Use SST file boundaries as approximate split points.
	sstInfos, err := db.SSTables()
	if err != nil {
		return status.Errorf(codes.Internal, "reading SSTs: %v", err)
	}

	var offsetBytes int64
	var dec CellDecoder
	for _, level := range sstInfos {
		for _, sst := range level {
			if len(sst.Smallest.UserKey) > 0 {
				rowKey, _, _, _, ok := dec.Decode(sst.Smallest.UserKey)
				if !ok || len(rowKey) == 0 {
					continue
				}
				rk := append([]byte(nil), rowKey...)
				if err := stream.Send(&bigtablepb.SampleRowKeysResponse{
					RowKey:      rk,
					OffsetBytes: offsetBytes,
				}); err != nil {
					return err
				}
			}
			// Use saturating addition to avoid capping at MaxInt64 permanently.
			remaining := math.MaxInt64 - offsetBytes
			if sst.Size > uint64(remaining) {
				offsetBytes = math.MaxInt64
			} else {
				offsetBytes += int64(sst.Size) //nolint:gosec
			}
		}
	}

	return nil
}
