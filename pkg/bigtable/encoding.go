// Package bigtable implements a Google Cloud Bigtable v2-compatible gRPC server.
package bigtable

import (
	"encoding/binary"
	"math"
)

// Key encoding format:
//
//	[row_len:2][row_key][0x00][family_len:1][family][0x00][qual_len:2][qualifier][0x00][inverted_ts:8]
//
// Components are length-prefixed and separated by 0x00 sentinel bytes.
// The inverted timestamp (MaxInt64 - timestamp) ensures newest cells sort
// first within a column under Pebble's ascending-by-default comparer.
//
// Overhead per cell: 2 + 1 + 1 + 2 + 1 + 8 = 15 bytes.

// invertedTimestamp converts a Bigtable timestamp (microseconds) to the
// inverted form used in Pebble keys. Inverted timestamps sort newest first
// since larger timestamps produce smaller inverted values.
//
// The subtraction MaxInt64 - timestampMicros may overflow int64 when the
// timestamp is negative; this is intentional. The overflow maps negative
// timestamps to values > MaxInt64 in the uint64 space, preserving the
// total ordering required by Bigtable's key encoding.
func invertedTimestamp(timestampMicros int64) uint64 {
	return uint64(math.MaxInt64 - timestampMicros) //nolint:gosec
}

// timestampFromInverted converts a Pebble inverted timestamp back to a
// Bigtable timestamp.
//
// The int64(inv) conversion may overflow when inv > MaxInt64 (negative
// timestamps). This is the inverse of invertedTimestamp and is required
// for correct round-trip encoding.
func timestampFromInverted(inv uint64) int64 {
	return math.MaxInt64 - int64(inv) //nolint:gosec
}

// encodeRowPrefix returns the key prefix that identifies a row:
//
//	[row_len:2][row_key][0x00]
func encodeRowPrefix(rowKey []byte) []byte {
	if len(rowKey) > math.MaxUint16 {
		return nil
	}
	buf := make([]byte, 2+len(rowKey)+1)
	// G115: bounds-checked above to guarantee len(rowKey) ≤ MaxUint16.
	binary.BigEndian.PutUint16(buf, uint16(len(rowKey))) //nolint:gosec
	copy(buf[2:], rowKey)
	buf[2+len(rowKey)] = 0x00
	return buf
}

// encodeFamilyPrefix returns the key prefix that identifies a column family
// within a row:
//
//	[row_prefix][family_len:1][family][0x00]
func encodeFamilyPrefix(rowPrefix []byte, family string) []byte {
	if len(family) > math.MaxUint8 {
		return nil
	}
	buf := make([]byte, len(rowPrefix)+1+len(family)+1)
	copy(buf, rowPrefix)
	// G115: bounds-checked above to guarantee len(family) ≤ MaxUint8.
	buf[len(rowPrefix)] = byte(len(family)) //nolint:gosec
	copy(buf[len(rowPrefix)+1:], family)
	buf[len(rowPrefix)+1+len(family)] = 0x00
	return buf
}

// encodeColumnPrefix returns the key prefix that identifies a column (family +
// qualifier) within a row:
//
//	[family_prefix][qual_len:2][qualifier][0x00]
func encodeColumnPrefix(familyPrefix []byte, qualifier []byte) []byte {
	if len(qualifier) > math.MaxUint16 {
		return nil
	}
	buf := make([]byte, len(familyPrefix)+2+len(qualifier)+1)
	copy(buf, familyPrefix)
	// G115: bounds-checked above to guarantee len(qualifier) ≤ MaxUint16.
	binary.BigEndian.PutUint16(buf[len(familyPrefix):], uint16(len(qualifier))) //nolint:gosec
	copy(buf[len(familyPrefix)+2:], qualifier)
	buf[len(familyPrefix)+2+len(qualifier)] = 0x00
	return buf
}

// EncodeCellKey encodes a full Pebble key for a Bigtable cell.
func EncodeCellKey(rowKey []byte, family string, qualifier []byte, timestampMicros int64) []byte {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, family)
	cp := encodeColumnPrefix(fp, qualifier)

	buf := make([]byte, len(cp)+8)
	copy(buf, cp)
	binary.BigEndian.PutUint64(buf[len(cp):], invertedTimestamp(timestampMicros))
	return buf
}

// DecodeCellKey decodes a Pebble key back into its Bigtable components.
// Returns false if the key format is invalid.
func DecodeCellKey(key []byte) (rowKey []byte, family string, qualifier []byte, timestampMicros int64, ok bool) {
	// Parse row key length.
	if len(key) < 3 {
		return nil, "", nil, 0, false
	}
	rowLen := int(binary.BigEndian.Uint16(key))
	if 2+rowLen+1 > len(key) {
		return nil, "", nil, 0, false
	}
	if key[2+rowLen] != 0x00 {
		return nil, "", nil, 0, false
	}
	rowKey = make([]byte, rowLen)
	copy(rowKey, key[2:2+rowLen])

	// Parse family name.
	pos := 2 + rowLen + 1 // after row prefix + sentinel
	if pos+1 > len(key) || key[pos] == 0x00 {
		return nil, "", nil, 0, false
	}
	famLen := int(key[pos])
	pos++
	if pos+famLen+1 > len(key) {
		return nil, "", nil, 0, false
	}
	family = string(key[pos : pos+famLen])
	pos += famLen
	if key[pos] != 0x00 {
		return nil, "", nil, 0, false
	}
	pos++ // skip sentinel

	// Parse qualifier.
	if pos+2 > len(key) {
		return nil, "", nil, 0, false
	}
	qualLen := int(binary.BigEndian.Uint16(key[pos:]))
	pos += 2
	if pos+qualLen+1+8 > len(key) {
		return nil, "", nil, 0, false
	}
	qualifier = make([]byte, qualLen)
	copy(qualifier, key[pos:pos+qualLen])
	pos += qualLen
	if key[pos] != 0x00 {
		return nil, "", nil, 0, false
	}
	pos++ // skip sentinel

	// Parse inverted timestamp.
	if pos+8 != len(key) {
		return nil, "", nil, 0, false
	}
	invTS := binary.BigEndian.Uint64(key[pos:])
	timestampMicros = timestampFromInverted(invTS)

	return rowKey, family, qualifier, timestampMicros, true
}

// rowEndKey returns the exclusive end key for a row prefix used by DeleteRange.
// The end key is row prefix + [0xFF], which is guaranteed to be lexicographically
// greater than any cell key in this row since the byte after the row prefix
// is family_len (≤ 0x40).
func rowEndKey(rowPrefix []byte) []byte {
	buf := make([]byte, len(rowPrefix)+1)
	copy(buf, rowPrefix)
	buf[len(rowPrefix)] = 0xFF
	return buf
}

// familyEndKey returns the exclusive end key for a family prefix.
func familyEndKey(familyPrefix []byte) []byte {
	buf := make([]byte, len(familyPrefix)+1)
	copy(buf, familyPrefix)
	buf[len(familyPrefix)] = 0xFF
	return buf
}

// columnEndKey returns the exclusive end key for a column prefix (all timestamps).
func columnEndKey(columnPrefix []byte) []byte {
	buf := make([]byte, len(columnPrefix)+1)
	copy(buf, columnPrefix)
	buf[len(columnPrefix)] = 0xFF
	return buf
}

// encodeTimestampRangeBounds returns start and end keys for a column prefix
// within a specific timestamp range. Bigtable's TimestampRange specifies
// start_timestamp_micros (inclusive) and end_timestamp_micros (exclusive).
// Since timestamps are inverted, larger timestamps produce smaller inverted
// values. For ts ∈ [startTS, endTS) the inverted range is:
//
//	MaxInt64-endTS < inv <= MaxInt64-startTS
//
// Which maps to Pebble bounds [col+inv(endTS)+1, col+inv(startTS)).
// The +1 on endTS ensures the exclusive end of the Bigtable range maps to
// the inclusive start of the Pebble range, so cells at exactly endTS are
// excluded. This addition is safe because endTS is always > 0 when called
// (the caller substitutes time.Now().UnixMicro() for zero).
func encodeTimestampRangeBounds(columnPrefix []byte, startTimestampMicros, endTimestampMicros int64) (start, end []byte) {
	startInv := invertedTimestamp(startTimestampMicros)
	endInv := invertedTimestamp(endTimestampMicros)

	// Skip the cell at exactly endTS.
	shiftedEnd := endInv + 1

	startKey := make([]byte, len(columnPrefix)+8)
	copy(startKey, columnPrefix)
	binary.BigEndian.PutUint64(startKey[len(columnPrefix):], shiftedEnd)

	endKey := make([]byte, len(columnPrefix)+8)
	copy(endKey, columnPrefix)
	binary.BigEndian.PutUint64(endKey[len(columnPrefix):], startInv)

	return startKey, endKey
}

// rowKeyRangeBounds returns [start, end) bounds for scanning all cells of a
// specific row key. start = row prefix, end = row prefix + 0xFF.
func rowKeyRangeBounds(rowKey []byte) (start, end []byte) {
	rp := encodeRowPrefix(rowKey)
	return rp, rowEndKey(rp)
}

// rowKeyFamilyBounds returns [start, end) bounds for scanning all cells of a
// specific row key + family.
func rowKeyFamilyBounds(rowKey []byte, family string) (start, end []byte) {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, family)
	return fp, familyEndKey(fp)
}

// rowKeyColumnBounds returns [start, end) bounds for scanning all cells of a
// specific row + family + qualifier (all timestamps).
func rowKeyColumnBounds(rowKey []byte, family string, qualifier []byte) (start, end []byte) {
	rp := encodeRowPrefix(rowKey)
	fp := encodeFamilyPrefix(rp, family)
	cp := encodeColumnPrefix(fp, qualifier)
	return cp, columnEndKey(cp)
}

// keyHasRowPrefix reports whether the given key starts with the row prefix
// encoding for the specified row key.
func keyHasRowPrefix(key, rowKey []byte) bool {
	rp := encodeRowPrefix(rowKey)
	return len(key) >= len(rp) && string(key[:len(rp)]) == string(rp)
}
