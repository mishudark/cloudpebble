package bigtable

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// MutateRow applies a set of mutations to a single row atomically.
func (s *Server) MutateRow(ctx context.Context, req *bigtablepb.MutateRowRequest) (*bigtablepb.MutateRowResponse, error) {
	rowKey := req.GetRowKey()
	if len(rowKey) == 0 {
		return nil, status.Error(codes.InvalidArgument, "row_key is required")
	}
	if len(req.GetMutations()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "mutations is required")
	}

	eng, err := s.getEngine(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()
	batch := db.NewBatch()
	defer batch.Close()

	if err := applyMutationsToBatch(batch, rowKey, req.GetMutations()); err != nil {
		return nil, err
	}

	if err := eng.Apply(ctx, batch); err != nil {
		return nil, status.Errorf(codes.Internal, "applying mutations: %v", err)
	}

	return &bigtablepb.MutateRowResponse{}, nil
}

// MutateRows applies mutations to multiple rows in a batch.
func (s *Server) MutateRows(req *bigtablepb.MutateRowsRequest, stream grpc.ServerStreamingServer[bigtablepb.MutateRowsResponse]) error {
	entries := req.GetEntries()
	if len(entries) == 0 {
		return status.Error(codes.InvalidArgument, "entries is required")
	}

	eng, err := s.getEngine(stream.Context(), req.GetTableName())
	if err != nil {
		return status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()
	batch := db.NewBatch()
	defer batch.Close()

	var entryErrors []struct {
		index int64
		err   error
	}

	for i, entry := range entries {
		if err := applyMutationsToBatch(batch, entry.GetRowKey(), entry.GetMutations()); err != nil {
			entryErrors = append(entryErrors, struct {
				index int64
				err   error
			}{index: int64(i), err: err})
		}
	}

	if len(entryErrors) > 0 {
		resp := &bigtablepb.MutateRowsResponse{
			RateLimitInfo: &bigtablepb.RateLimitInfo{
				Period: &durationpb.Duration{Seconds: 1},
				Factor: 1.0,
			},
		}
		errSet := make(map[int64]bool)
		for _, e := range entryErrors {
			errSet[e.index] = true
			resp.Entries = append(resp.Entries, &bigtablepb.MutateRowsResponse_Entry{
				Index:  e.index,
				Status: toBigtableStatus(e.err),
			})
		}
		for i := range entries {
			if !errSet[int64(i)] {
				resp.Entries = append(resp.Entries, &bigtablepb.MutateRowsResponse_Entry{
					Index:  int64(i),
					Status: okStatus(),
				})
			}
		}
		return stream.Send(resp)
	}

	err = eng.Apply(stream.Context(), batch)
	if err != nil {
		resp := &bigtablepb.MutateRowsResponse{
			RateLimitInfo: &bigtablepb.RateLimitInfo{
				Period: &durationpb.Duration{Seconds: 1},
				Factor: 1.0,
			},
		}
		for i := range entries {
			resp.Entries = append(resp.Entries, &bigtablepb.MutateRowsResponse_Entry{
				Index:  int64(i),
				Status: toBigtableStatus(err),
			})
		}
		return stream.Send(resp)
	}

	resp := &bigtablepb.MutateRowsResponse{
		RateLimitInfo: &bigtablepb.RateLimitInfo{
			Period: &durationpb.Duration{Seconds: 1},
			Factor: 1.0,
		},
	}
	for i := range entries {
		resp.Entries = append(resp.Entries, &bigtablepb.MutateRowsResponse_Entry{
			Index:  int64(i),
			Status: okStatus(),
		})
	}
	return stream.Send(resp)
}

// CheckAndMutateRow atomically reads a row, evaluates a predicate filter,
// and applies mutations based on the result.
func (s *Server) CheckAndMutateRow(ctx context.Context, req *bigtablepb.CheckAndMutateRowRequest) (*bigtablepb.CheckAndMutateRowResponse, error) {
	rowKey := req.GetRowKey()
	if len(rowKey) == 0 {
		return nil, status.Error(codes.InvalidArgument, "row_key is required")
	}

	eng, err := s.getEngine(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()
	predicateFilter := req.GetPredicateFilter()

	// Evaluate predicate: does the row match the filter?
	predicateMatched := rowHasCells(db, rowKey, predicateFilter)

	mutations := req.GetTrueMutations()
	if !predicateMatched {
		mutations = req.GetFalseMutations()
	}

	if len(mutations) == 0 {
		return &bigtablepb.CheckAndMutateRowResponse{PredicateMatched: predicateMatched}, nil
	}

	batch := db.NewBatch()
	defer batch.Close()

	if err := applyMutationsToBatch(batch, rowKey, mutations); err != nil {
		return nil, err
	}

	if err := eng.Apply(ctx, batch); err != nil {
		return nil, status.Errorf(codes.Internal, "applying mutations: %v", err)
	}

	return &bigtablepb.CheckAndMutateRowResponse{PredicateMatched: predicateMatched}, nil
}

// rowHasCells checks whether the given row has any cells matching the filter.
// If filter is nil, returns true if the row has any cells.
func rowHasCells(db *pebble.DB, rowKey []byte, filter *bigtablepb.RowFilter) bool {
	start, end := rowKeyRangeBounds(rowKey)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return false
	}
	defer iter.Close()

	if filter == nil {
		return iter.First() && iter.Valid()
	}

	engine, err := newRowFilterEngine(filter)
	if err != nil {
		// Can't evaluate filter; fall back to checking any cells exist.
		return iter.First() && iter.Valid()
	}
	return engine.hasMatch(iter)
}

// applyMutationsToBatch applies a list of Bigtable mutations to a Pebble batch.
func applyMutationsToBatch(batch *pebble.Batch, rowKey []byte, mutations []*bigtablepb.Mutation) error {
	for _, mut := range mutations {
		if err := applyMutationToBatch(batch, rowKey, mut); err != nil {
			return err
		}
	}
	return nil
}

// applyMutationToBatch applies a single Bigtable mutation to a Pebble batch.
func applyMutationToBatch(batch *pebble.Batch, rowKey []byte, mut *bigtablepb.Mutation) error {
	switch m := mut.Mutation.(type) {
	case *bigtablepb.Mutation_SetCell_:
		return applySetCell(batch, rowKey, m.SetCell)

	case *bigtablepb.Mutation_DeleteFromColumn_:
		return applyDeleteFromColumn(batch, rowKey, m.DeleteFromColumn)

	case *bigtablepb.Mutation_DeleteFromFamily_:
		return applyDeleteFromFamily(batch, rowKey, m.DeleteFromFamily)

	case *bigtablepb.Mutation_DeleteFromRow_:
		return applyDeleteFromRow(batch, rowKey)

	case *bigtablepb.Mutation_AddToCell_:
		return status.Error(codes.Unimplemented, "AddToCell not supported")

	case *bigtablepb.Mutation_MergeToCell_:
		return status.Error(codes.Unimplemented, "MergeToCell not supported")

	default:
		return status.Error(codes.InvalidArgument, "unknown mutation type")
	}
}

func applySetCell(batch *pebble.Batch, rowKey []byte, sc *bigtablepb.Mutation_SetCell) error {
	ts := sc.GetTimestampMicros()
	if ts == -1 {
		ts = time.Now().UnixMicro()
	}
	key := EncodeCellKey(rowKey, sc.GetFamilyName(), sc.GetColumnQualifier(), ts)
	return batch.Set(key, sc.GetValue(), nil)
}

func applyDeleteFromColumn(batch *pebble.Batch, rowKey []byte, dc *bigtablepb.Mutation_DeleteFromColumn) error {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, dc.GetFamilyName())
	cp := encodeColumnPrefix(fp, dc.GetColumnQualifier())

	tr := dc.GetTimeRange()
	if tr != nil {
		startTS := tr.GetStartTimestampMicros()
		endTS := tr.GetEndTimestampMicros()
		if endTS == 0 {
			endTS = time.Now().UnixMicro()
		}
		start, end := encodeTimestampRangeBounds(cp, startTS, endTS)
		return batch.DeleteRange(start, end, nil)
	}
	return batch.DeleteRange(cp, columnEndKey(cp), nil)
}

func applyDeleteFromFamily(batch *pebble.Batch, rowKey []byte, df *bigtablepb.Mutation_DeleteFromFamily) error {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, df.GetFamilyName())
	return batch.DeleteRange(fp, familyEndKey(fp), nil)
}

func applyDeleteFromRow(batch *pebble.Batch, rowKey []byte) error {
	rp := encodeRowPrefix(rowKey)
	return batch.DeleteRange(rp, rowEndKey(rp), nil)
}

// toBigtableStatus converts a Go error to a rpcstatus.Status.
func toBigtableStatus(err error) *rpcstatus.Status {
	s, _ := status.FromError(err)
	return &rpcstatus.Status{
		Code:    int32(s.Code()),
		Message: s.Message(),
	}
}

// okStatus returns a rpcstatus.Status representing OK.
func okStatus() *rpcstatus.Status {
	return &rpcstatus.Status{Code: int32(codes.OK)}
}
