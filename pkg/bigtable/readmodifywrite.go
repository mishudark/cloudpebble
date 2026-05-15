package bigtable

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadModifyWriteRow atomically reads a row, applies read-modify-write rules,
// and writes the modified values back. Earlier rules affect the results of
// later ones because reads are performed on an indexed batch.
func (s *Server) ReadModifyWriteRow(ctx context.Context, req *bigtablepb.ReadModifyWriteRowRequest) (*bigtablepb.ReadModifyWriteRowResponse, error) {
	rowKey := req.GetRowKey()
	if len(rowKey) == 0 {
		return nil, status.Error(codes.InvalidArgument, "row_key is required")
	}
	if len(req.GetRules()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "rules is required")
	}

	eng, err := s.getEngine(ctx, req.GetTableName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "opening table: %v", err)
	}

	db := eng.DB()
	batch := db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	ts := time.Now().UnixMicro()

	// Track the final values of modified cells for the response.
	// family -> qualifier -> final value
	modified := make(map[string]map[string][]byte)

	for _, rule := range req.GetRules() {
		family := rule.GetFamilyName()
		qualifier := rule.GetColumnQualifier()

		// Read the current latest value for this column from the batch.
		// Because this is an indexed batch, earlier writes in this loop
		// are visible to subsequent reads.
		currentValue := readCellValue(batch, rowKey, family, qualifier)

		var newValue []byte
		switch r := rule.GetRule().(type) {
		case *bigtablepb.ReadModifyWriteRule_AppendValue:
			newValue = append(currentValue, r.AppendValue...)
		case *bigtablepb.ReadModifyWriteRule_IncrementAmount:
			var currentInt int64
			if len(currentValue) > 0 {
				if len(currentValue) != 8 {
					return nil, status.Error(codes.FailedPrecondition, "existing cell value must be 8 bytes for increment")
				}
				uv := binary.BigEndian.Uint64(currentValue)
				if uv > math.MaxInt64 {
					return nil, status.Error(codes.FailedPrecondition, "existing cell value exceeds int64 range")
				}
				currentInt = int64(uv)
			}
			newInt := currentInt + r.IncrementAmount
			if newInt < 0 {
				return nil, status.Error(codes.OutOfRange, "increment result is negative")
			}
			newValue = make([]byte, 8)
			binary.BigEndian.PutUint64(newValue, uint64(newInt))
		default:
			return nil, status.Error(codes.InvalidArgument, "unknown rule type")
		}

		key := EncodeCellKey(rowKey, family, qualifier, ts)
		if err := batch.Set(key, newValue, nil); err != nil {
			return nil, status.Errorf(codes.Internal, "batch set: %v", err)
		}

		if modified[family] == nil {
			modified[family] = make(map[string][]byte)
		}
		modified[family][string(qualifier)] = newValue
	}

	if err := eng.Apply(ctx, batch); err != nil {
		return nil, status.Errorf(codes.Internal, "applying batch: %v", err)
	}

	// Build response row containing the new contents of all modified cells.
	row := &bigtablepb.Row{
		Key: append([]byte(nil), rowKey...),
	}
	for family, quals := range modified {
		fam := &bigtablepb.Family{Name: family}
		for qual, value := range quals {
			fam.Columns = append(fam.Columns, &bigtablepb.Column{
				Qualifier: []byte(qual),
				Cells: []*bigtablepb.Cell{{
					TimestampMicros: ts,
					Value:           append([]byte(nil), value...),
				}},
			})
		}
		row.Families = append(row.Families, fam)
	}

	return &bigtablepb.ReadModifyWriteRowResponse{Row: row}, nil
}

// readCellValue reads the latest value for a specific column from the batch.
// Returns nil if the cell does not exist.
func readCellValue(batch *pebble.Batch, rowKey []byte, family string, qualifier []byte) []byte {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, family)
	cp := encodeColumnPrefix(fp, qualifier)

	iter, err := batch.NewIter(&pebble.IterOptions{
		LowerBound: cp,
		UpperBound: columnEndKey(cp),
	})
	if err != nil {
		return nil
	}
	defer func() { _ = iter.Close() }()

	if iter.First() && iter.Valid() {
		// Verify this key still belongs to the requested column.
		if bytes.HasPrefix(iter.Key(), cp) {
			val := make([]byte, len(iter.Value()))
			copy(val, iter.Value())
			return val
		}
	}
	return nil
}
