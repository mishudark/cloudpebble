package engine

import (
	"encoding/binary"
	"io"
	"strings"

	"github.com/cockroachdb/pebble"
)

// int64SumMergerName is the on-disk name of the engine's default merger.
// Pebble records the merger name in the OPTIONS file; it must remain stable
// across releases for stores created with this merger.
const int64SumMergerName = "cloudpebble.int64sum.v1"

// int64SumMerger sums 8-byte big-endian int64 operands. It backs Bigtable
// aggregate (counter) cells written with add_to_cell / merge_to_cell: deltas
// are written with Batch.Merge and combined during reads and compactions,
// giving atomic, commutative increments without read-modify-write races.
//
// Operands that are not exactly 8 bytes contribute zero. The Bigtable layer
// validates a cell before issuing its first Merge, so this only occurs when
// SetCell and aggregate mutations are mixed on the same cell, which is
// documented as unsupported.
var int64SumMerger = &pebble.Merger{
	Name: int64SumMergerName,
	Merge: func(_, value []byte) (pebble.ValueMerger, error) {
		m := &int64SumValueMerger{}
		m.add(value)
		return m, nil
	},
}

type int64SumValueMerger struct {
	sum int64
}

func (m *int64SumValueMerger) add(value []byte) {
	if len(value) == 8 {
		m.sum += int64(binary.BigEndian.Uint64(value)) //nolint:gosec // intentional two's-complement round-trip
	}
}

func (m *int64SumValueMerger) MergeNewer(value []byte) error {
	m.add(value)
	return nil
}

func (m *int64SumValueMerger) MergeOlder(value []byte) error {
	m.add(value)
	return nil
}

func (m *int64SumValueMerger) Finish(bool) ([]byte, io.Closer, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(m.sum)) //nolint:gosec // intentional two's-complement round-trip
	return buf, nil, nil
}

// EncodeInt64SumOperand encodes a delta for an int64-sum merge operand.
func EncodeInt64SumOperand(delta int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(delta)) //nolint:gosec // intentional two's-complement round-trip
	return buf
}

// isMergerMismatch reports whether err is Pebble's error for opening a store
// whose recorded merger name differs from the configured one.
func isMergerMismatch(err error) bool {
	return err != nil && strings.Contains(err.Error(), "merger name from file")
}

// AggregatesSupported reports whether the engine was opened with the
// int64-sum merger required by Bigtable aggregate mutations. It is false
// for local stores created before the merger became the engine default.
func (e *Engine) AggregatesSupported() bool {
	return !e.mergerFallback.Load()
}
