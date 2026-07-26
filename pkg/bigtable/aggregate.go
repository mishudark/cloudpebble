package bigtable

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"github.com/mishudark/cloudpebble/pkg/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// aggregateTimestampMicros is the timestamp used for all aggregate cells.
// Bigtable aggregates are timeless: every update to a counter targets the
// same cell so Pebble merge operands accumulate into one running total.
const aggregateTimestampMicros = 0

// applyAddToCell applies an AddToCell mutation: the input is accumulated
// into the cell as an int64-sum aggregate. The write is a Pebble merge
// operand, so concurrent adds are atomic and never lost.
func applyAddToCell(db *pebble.DB, batch *pebble.Batch, rowKey []byte, a *bigtablepb.Mutation_AddToCell) error {
	qualifier, delta, err := aggregateOperands(a.GetFamilyName(), a.GetColumnQualifier(), a.GetInput())
	if err != nil {
		return err
	}
	return mergeAggregate(db, batch, rowKey, a.GetFamilyName(), qualifier, delta)
}

// applyMergeToCell applies a MergeToCell mutation. For the int64-sum
// aggregator this is identical to AddToCell; a NULL input is a no-op.
func applyMergeToCell(db *pebble.DB, batch *pebble.Batch, rowKey []byte, m *bigtablepb.Mutation_MergeToCell) error {
	if m.GetInput() == nil || m.GetInput().GetKind() == nil {
		return nil // merging NULL has no effect
	}
	qualifier, delta, err := aggregateOperands(m.GetFamilyName(), m.GetColumnQualifier(), m.GetInput())
	if err != nil {
		return err
	}
	return mergeAggregate(db, batch, rowKey, m.GetFamilyName(), qualifier, delta)
}

// mergeAggregate validates the target cell and writes one int64-sum merge
// operand at the aggregate timestamp.
func mergeAggregate(db *pebble.DB, batch *pebble.Batch, rowKey []byte, family string, qualifier []byte, delta int64) error {
	// Reject cells that already hold a non-aggregate (non-int64) value so a
	// misuse of SetCell on an aggregate column fails loudly instead of being
	// silently treated as zero by the merger.
	if current := readCellValue(db, rowKey, family, qualifier); current != nil && len(current) != 8 {
		return status.Errorf(codes.FailedPrecondition,
			"cell %q:%q holds a %d-byte value, not an int64 aggregate", family, qualifier, len(current))
	}
	key := EncodeCellKey(rowKey, family, qualifier, aggregateTimestampMicros)
	return batch.Merge(key, engine.EncodeInt64SumOperand(delta), nil)
}

// aggregateOperands extracts and validates the qualifier and int64 input of
// an aggregate mutation.
func aggregateOperands(family string, qualifierValue, input *bigtablepb.Value) (qualifier []byte, delta int64, err error) {
	if len(family) > math.MaxUint8 {
		return nil, 0, status.Error(codes.InvalidArgument, "family name too long (max 255 bytes)")
	}
	qualifier, err = aggregateQualifier(qualifierValue)
	if err != nil {
		return nil, 0, err
	}
	if len(qualifier) > math.MaxUint16 {
		return nil, 0, status.Error(codes.InvalidArgument, "column qualifier too long (max 65535 bytes)")
	}
	delta, err = aggregateInt64Input(input)
	if err != nil {
		return nil, 0, err
	}
	return qualifier, delta, nil
}

// aggregateQualifier extracts the raw qualifier bytes from a Value. The proto
// requires a raw_value; bytes_value is accepted as a lenient equivalent.
func aggregateQualifier(v *bigtablepb.Value) ([]byte, error) {
	switch k := v.GetKind().(type) {
	case *bigtablepb.Value_RawValue:
		return k.RawValue, nil
	case *bigtablepb.Value_BytesValue:
		return k.BytesValue, nil
	}
	return nil, status.Error(codes.InvalidArgument, "aggregate column_qualifier must be a raw_value")
}

// aggregateInt64Input extracts an int64 from an aggregate mutation input.
// int_value is the canonical form; an 8-byte big-endian raw_value is accepted
// as the wire encoding of the same sum state.
func aggregateInt64Input(v *bigtablepb.Value) (int64, error) {
	switch k := v.GetKind().(type) {
	case *bigtablepb.Value_IntValue:
		return k.IntValue, nil
	case *bigtablepb.Value_RawValue:
		if len(k.RawValue) == 8 {
			return int64(binary.BigEndian.Uint64(k.RawValue)), nil //nolint:gosec // intentional two's-complement round-trip
		}
		return 0, status.Errorf(codes.InvalidArgument, "aggregate raw_value input must be 8 bytes, got %d", len(k.RawValue))
	}
	return 0, status.Error(codes.InvalidArgument, "aggregate input must be an int64 (sum aggregator)")
}
