// Package bigtable implements a Google Cloud Bigtable v2-compatible gRPC server.
package bigtable

import (
	"encoding/binary"
	"math"
	"slices"
)

// Key encoding format:
//
//	[escaped_row_key][0x00][0x00][family_len:1][family][0x00][qual_len:2][qualifier][0x00][inverted_ts:8]
//
// The row key uses null-escape encoding (0x00 → 0x00 0xFF) and is terminated
// by 0x00 0x00. This preserves lexicographic ordering of raw row keys so that
// forward/reverse scans and prefix ranges work correctly.
//
// Family and qualifier are length-prefixed and separated by 0x00 sentinel bytes.
// The inverted timestamp (MaxInt64 - timestamp) ensures newest cells sort
// first within a column under Pebble's ascending-by-default comparer.

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
//	[escaped_row_key][0x00][0x00]
//
// Null bytes (0x00) in the row key are escaped as 0x00 0xFF.
// The terminator 0x00 0x00 is guaranteed not to appear inside the escaped key.
// Writes directly into the result buffer, avoiding intermediate allocations.
func encodeRowPrefix(rowKey []byte) []byte {
	// Fast path: no null bytes in row key.
	if slices.Contains(rowKey, 0x00) {
		return encodeRowPrefixEscaped(rowKey)
	}
	buf := make([]byte, len(rowKey)+2)
	copy(buf, rowKey)
	buf[len(rowKey)] = 0x00
	buf[len(rowKey)+1] = 0x00
	return buf
}

// encodeRowPrefixEscaped handles row keys containing null bytes.
func encodeRowPrefixEscaped(rowKey []byte) []byte {
	// Count null bytes to compute escaped length.
	nullCount := 0
	for _, c := range rowKey {
		if c == 0x00 {
			nullCount++
		}
	}
	escapedLen := len(rowKey) + nullCount
	buf := make([]byte, escapedLen+2)
	pos := 0
	for _, c := range rowKey {
		if c == 0x00 {
			buf[pos] = 0x00
			buf[pos+1] = 0xFF
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}
	buf[escapedLen] = 0x00
	buf[escapedLen+1] = 0x00
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

// CellDecoder is a reusable decoder that decodes Pebble cell keys
// with minimal allocations. After warmup (one key per unique row-key
// length), Decode incurs zero heap allocations per call.
type CellDecoder struct {
	rowKeyBuf    []byte
	qualifierBuf []byte
}

// Decode decodes a Pebble key into its Bigtable components. The returned
// rowKey slice points into the original key and is only valid until the
// next iterator positioning operation. The qualifier is a copy into an
// internal buffer. Callers that need qualifier after the next iterator
// operation must copy it.
func (d *CellDecoder) Decode(key []byte) (rowKey []byte, family string, qualifier []byte, timestampMicros int64, ok bool) {
	// Scan for row terminator (0x00 0x00) and count escapes.
	pos := 0
	esc := 0
	for {
		if pos >= len(key) {
			return
		}
		if key[pos] != 0x00 {
			pos++
			continue
		}
		if pos+1 >= len(key) {
			return
		}
		if key[pos+1] == 0xFF {
			esc++
			pos += 2
			continue
		}
		if key[pos+1] == 0x00 {
			break
		}
		return
	}
	rowLen := pos
	pos += 2

	if esc > 0 {
		d.rowKeyBuf = append(d.rowKeyBuf[:0], make([]byte, rowLen-esc)...)
		dst := 0
		for src := 0; src < rowLen; src++ {
			if src+1 < rowLen && key[src] == 0x00 && key[src+1] == 0xFF {
				d.rowKeyBuf[dst] = 0x00
				src++
			} else {
				d.rowKeyBuf[dst] = key[src]
			}
			dst++
		}
		rowKey = d.rowKeyBuf
	} else {
		rowKey = key[:rowLen]
	}

	// Parse family name.
	if pos >= len(key) {
		return
	}
	famLen := int(key[pos])
	pos++
	if pos+famLen >= len(key) || key[pos+famLen] != 0x00 {
		return
	}
	family = string(key[pos : pos+famLen])
	pos += famLen + 1

	// Parse qualifier (copied into reusable buffer).
	if pos+2 > len(key) {
		return
	}
	qualLen := int(binary.BigEndian.Uint16(key[pos:]))
	pos += 2
	if pos+qualLen >= len(key) || key[pos+qualLen] != 0x00 {
		return
	}
	d.qualifierBuf = append(d.qualifierBuf[:0], key[pos:pos+qualLen]...)
	qualifier = d.qualifierBuf
	pos += qualLen + 1

	// Parse inverted timestamp.
	if pos+8 != len(key) {
		return
	}
	invTS := binary.BigEndian.Uint64(key[pos:])
	timestampMicros = timestampFromInverted(invTS)

	ok = true
	return
}

// DecodeCellKey decodes a Pebble key back into its Bigtable components.
// Returns false if the key format is invalid. The returned rowKey and
// qualifier slices are freshly allocated copies safe to retain.
//
// For hot-path use, prefer CellDecoder.Decode for zero-alloc decoding.
func DecodeCellKey(key []byte) (rowKey []byte, family string, qualifier []byte, timestampMicros int64, ok bool) {
	// Scan for row terminator (0x00 0x00) and count escapes.
	pos := 0
	esc := 0
	for {
		if pos >= len(key) {
			return
		}
		if key[pos] != 0x00 {
			pos++
			continue
		}
		if pos+1 >= len(key) {
			return
		}
		if key[pos+1] == 0xFF {
			esc++
			pos += 2
			continue
		}
		if key[pos+1] == 0x00 {
			break
		}
		return
	}
	rowLen := pos
	pos += 2

	if esc > 0 {
		rowKey = make([]byte, rowLen-esc)
		dst := 0
		for src := 0; src < rowLen; src++ {
			if src+1 < rowLen && key[src] == 0x00 && key[src+1] == 0xFF {
				rowKey[dst] = 0x00
				src++
			} else {
				rowKey[dst] = key[src]
			}
			dst++
		}
	} else {
		rowKey = append([]byte(nil), key[:rowLen]...)
	}

	// Parse family name.
	if pos >= len(key) {
		return
	}
	famLen := int(key[pos])
	pos++
	if pos+famLen >= len(key) || key[pos+famLen] != 0x00 {
		return
	}
	family = string(key[pos : pos+famLen])
	pos += famLen + 1

	// Parse qualifier.
	if pos+2 > len(key) {
		return
	}
	qualLen := int(binary.BigEndian.Uint16(key[pos:]))
	pos += 2
	if pos+qualLen >= len(key) || key[pos+qualLen] != 0x00 {
		return
	}
	qualifier = make([]byte, qualLen)
	copy(qualifier, key[pos:pos+qualLen])
	pos += qualLen + 1

	// Parse inverted timestamp.
	if pos+8 != len(key) {
		return
	}
	invTS := binary.BigEndian.Uint64(key[pos:])
	timestampMicros = timestampFromInverted(invTS)

	ok = true
	return
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
// Which maps to Pebble bounds [col+inv(endTS)+1, col+inv(startTS)+1).
// The +1 on endTS ensures the exclusive end of the Bigtable range maps to
// the inclusive start of the Pebble range, so cells at exactly endTS are
// excluded. The +1 on startTS ensures cells at exactly startTS are included
// (Pebble DeleteRange's end is exclusive). This is safe because
// inverted timestamps for non-negative timestamps are <= MaxInt64, so
// adding 1 cannot overflow uint64.
func encodeTimestampRangeBounds(columnPrefix []byte, startTimestampMicros, endTimestampMicros int64) (start, end []byte) {
	startInv := invertedTimestamp(startTimestampMicros)
	endInv := invertedTimestamp(endTimestampMicros)

	// Skip the cell at exactly endTS. Use saturating arithmetic to
	// handle the case where endInv == math.MaxUint64 (negative timestamps).
	shiftedEnd := endInv + 1
	if shiftedEnd == 0 {
		shiftedEnd = math.MaxUint64
	}

	startKey := make([]byte, len(columnPrefix)+8)
	copy(startKey, columnPrefix)
	binary.BigEndian.PutUint64(startKey[len(columnPrefix):], shiftedEnd)

	endShifted := startInv + 1
	if endShifted == 0 {
		endShifted = math.MaxUint64
	}
	endKey := make([]byte, len(columnPrefix)+8)
	copy(endKey, columnPrefix)
	binary.BigEndian.PutUint64(endKey[len(columnPrefix):], endShifted)

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
