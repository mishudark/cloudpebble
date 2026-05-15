package bigtable

import (
	"bytes"
	"testing"
)

func FuzzDecodeCellKey(f *testing.F) {
	// Seed corpus with valid and invalid keys.
	validKey := EncodeCellKey([]byte("row1"), "cf", []byte("qual"), 1000000)
	f.Add(validKey)
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x01, 'a'})
	f.Add([]byte{0x00, 0x00, 'a', 0x00})
	f.Add([]byte{0x00, 0x01, 'a', 0x00, 0x01, 'c', 0x00, 0x00, 0x01, 'q', 0x00})

	// Add keys with various edge cases.
	f.Add(EncodeCellKey([]byte{}, "f", []byte{}, 0))
	f.Add(EncodeCellKey([]byte{0xFF, 0x00, 0x01}, "fam", []byte{0x00, 0xFF}, 0))
	f.Add(EncodeCellKey([]byte("row"), "cf", []byte("q"), 0))
	f.Add(EncodeCellKey([]byte("row"), "cf", []byte("q"), -1))

	f.Fuzz(func(t *testing.T, data []byte) {
		rowKey, family, qualifier, ts, ok := DecodeCellKey(data)
		if !ok {
			return
		}

		// If decoding succeeded, verify round-trip: re-encoding should produce the same key.
		encoded := EncodeCellKey(rowKey, family, qualifier, ts)
		if !bytes.Equal(encoded, data) {
			t.Errorf("round-trip mismatch: got %v, want %v", encoded, data)
		}

		// Verify the decoded key has valid structure.
		if len(data) < 15 {
			t.Errorf("valid key must be at least 15 bytes, got %d", len(data))
		}
	})
}

func FuzzEncodeCellKeyRoundTrip(f *testing.F) {
	f.Add([]byte("row"), "cf", []byte("qual"), int64(1000000))
	f.Add([]byte{}, "f", []byte{}, int64(0))
	f.Add([]byte{0x00, 0xFF, 0x01}, "family", []byte{0x00, 0x01, 0xFF}, int64(-1))
	f.Add([]byte("a"), "a", []byte("a"), int64(0))
	f.Add([]byte("test-row-key"), "column-family", []byte("some-qualifier"), int64(9223372036854775807))

	f.Fuzz(func(t *testing.T, rowKey []byte, family string, qualifier []byte, ts int64) {
		// Limit input sizes to avoid excessive allocations.
		if len(rowKey) > 4096 {
			rowKey = rowKey[:4096]
		}
		if len(family) > 64 {
			family = family[:64]
		}
		if len(qualifier) > 16384 {
			qualifier = qualifier[:16384]
		}
		if family == "" {
			family = "f"
		}

		key := EncodeCellKey(rowKey, family, qualifier, ts)
		dRow, dFamily, dQual, dTS, ok := DecodeCellKey(key)
		if !ok {
			t.Fatalf("DecodeCellKey failed on encoded key")
		}
		if !bytes.Equal(dRow, rowKey) {
			t.Errorf("rowKey mismatch: got %v, want %v", dRow, rowKey)
		}
		if dFamily != family {
			t.Errorf("family mismatch: got %q, want %q", dFamily, family)
		}
		if !bytes.Equal(dQual, qualifier) {
			t.Errorf("qualifier mismatch: got %v, want %v", dQual, qualifier)
		}
		if dTS != ts {
			t.Errorf("timestamp mismatch: got %d, want %d", dTS, ts)
		}
	})
}

func FuzzKeySortOrder(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), int64(100), int64(200))
	f.Add([]byte("row1"), []byte("row2"), int64(0), int64(0))
	f.Add([]byte{0x00}, []byte{0xFF}, int64(-1), int64(1))

	f.Fuzz(func(t *testing.T, rowA, rowB []byte, tsA, tsB int64) {
		if len(rowA) > 4096 {
			rowA = rowA[:4096]
		}
		if len(rowB) > 4096 {
			rowB = rowB[:4096]
		}

		keyA := EncodeCellKey(rowA, "cf", []byte("q"), tsA)
		keyB := EncodeCellKey(rowB, "cf", []byte("q"), tsB)

		// Same row key, newer timestamp should sort first.
		if bytes.Equal(rowA, rowB) && tsA > tsB && bytes.Compare(keyA, keyB) >= 0 {
			t.Errorf("newer timestamp should sort first")
		}
		if bytes.Equal(rowA, rowB) && tsA < tsB && bytes.Compare(keyA, keyB) <= 0 {
			t.Errorf("older timestamp should sort last")
		}
		if bytes.Equal(rowA, rowB) && tsA == tsB && !bytes.Equal(keyA, keyB) {
			t.Errorf("same row+ts should produce equal keys")
		}

		// Different row keys: encoded keys should not be equal.
		if !bytes.Equal(rowA, rowB) && bytes.Equal(keyA, keyB) {
			t.Errorf("different row keys should produce different encoded keys")
		}
	})
}

func FuzzTimestampRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(-1))
	f.Add(int64(9223372036854775807))
	f.Add(int64(-9223372036854775808))

	f.Fuzz(func(t *testing.T, ts int64) {
		inv := invertedTimestamp(ts)
		back := timestampFromInverted(inv)
		if back != ts {
			t.Errorf("timestamp round-trip: got %d, want %d", back, ts)
		}
	})
}

func FuzzRowPrefixBounds(f *testing.F) {
	f.Add([]byte("row"))
	f.Add([]byte{})
	f.Add([]byte{0x00, 0xFF, 0x01})

	f.Fuzz(func(t *testing.T, rowKey []byte) {
		if len(rowKey) > 4096 {
			rowKey = rowKey[:4096]
		}

		start, end := rowKeyRangeBounds(rowKey)
		if bytes.Compare(start, end) >= 0 {
			t.Errorf("start >= end for row bounds")
		}

		// Cell key should be within bounds.
		cellKey := EncodeCellKey(rowKey, "cf", []byte("q"), 100)
		if bytes.Compare(cellKey, start) < 0 || bytes.Compare(cellKey, end) >= 0 {
			t.Errorf("cell key not within row bounds")
		}

		// Family bounds.
		fStart, fEnd := rowKeyFamilyBounds(rowKey, "cf")
		if bytes.Compare(fStart, fEnd) >= 0 {
			t.Errorf("fStart >= fEnd for family bounds")
		}
		fCell := EncodeCellKey(rowKey, "cf", []byte("q"), 100)
		if bytes.Compare(fCell, fStart) < 0 || bytes.Compare(fCell, fEnd) >= 0 {
			t.Errorf("cell key not within family bounds")
		}

		// Column bounds.
		cStart, cEnd := rowKeyColumnBounds(rowKey, "cf", []byte("q"))
		if bytes.Compare(cStart, cEnd) >= 0 {
			t.Errorf("cStart >= cEnd for column bounds")
		}
		cCell := EncodeCellKey(rowKey, "cf", []byte("q"), 100)
		if bytes.Compare(cCell, cStart) < 0 || bytes.Compare(cCell, cEnd) >= 0 {
			t.Errorf("cell key not within column bounds")
		}
	})
}

func FuzzTimestampRangeBounds(f *testing.F) {
	f.Add(int64(100), int64(200))
	f.Add(int64(0), int64(10))
	f.Add(int64(-1000), int64(1000))

	f.Fuzz(func(t *testing.T, startTS, endTS int64) {
		cp := encodeColumnPrefix(
			encodeFamilyPrefix(encodeRowPrefix([]byte("row")), "cf"),
			[]byte("q"),
		)

		// Ensure endTS > startTS + 1 for a valid non-empty range.
		// A range of [startTS, startTS+1) contains no integer microseconds.
		if endTS <= startTS+1 {
			endTS = startTS + 2
		}

		start, end := encodeTimestampRangeBounds(cp, startTS, endTS)
		if bytes.Compare(start, end) >= 0 {
			t.Errorf("start >= end for timestamp range bounds")
		}

		// Cell within range should be in bounds.
		midTS := startTS + (endTS-startTS)/2
		cellIn := EncodeCellKey([]byte("row"), "cf", []byte("q"), midTS)
		if bytes.Compare(cellIn, start) < 0 || bytes.Compare(cellIn, end) >= 0 {
			t.Errorf("cell at ts=%d not within [%d, %d) bounds", midTS, startTS, endTS)
		}

		// Cell before range should NOT be in bounds.
		cellBefore := EncodeCellKey([]byte("row"), "cf", []byte("q"), startTS-1)
		if bytes.Compare(cellBefore, start) >= 0 && bytes.Compare(cellBefore, end) < 0 {
			t.Errorf("cell before range should not be in bounds")
		}

		// Cell at endTS should NOT be in bounds (exclusive).
		cellAtEnd := EncodeCellKey([]byte("row"), "cf", []byte("q"), endTS)
		if bytes.Compare(cellAtEnd, start) >= 0 && bytes.Compare(cellAtEnd, end) < 0 {
			t.Errorf("cell at endTS should not be in bounds (exclusive)")
		}
	})
}

func FuzzKeyHasRowPrefix(f *testing.F) {
	f.Add([]byte("row"), []byte("row"))
	f.Add([]byte("row"), []byte("ro"))
	f.Add([]byte("a"), []byte("b"))

	f.Fuzz(func(t *testing.T, rowKey, prefixKey []byte) {
		if len(rowKey) > 4096 {
			rowKey = rowKey[:4096]
		}
		if len(prefixKey) > 4096 {
			prefixKey = prefixKey[:4096]
		}

		key := EncodeCellKey(rowKey, "cf", []byte("q"), 100)
		hasPrefix := keyHasRowPrefix(key, prefixKey)

		// Should only match exact row key.
		expected := bytes.Equal(rowKey, prefixKey)
		if hasPrefix != expected {
			t.Errorf("keyHasRowPrefix: got %v, want %v for rowKey=%v prefixKey=%v", hasPrefix, expected, rowKey, prefixKey)
		}
	})
}
