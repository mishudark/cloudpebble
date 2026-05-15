package bigtable

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestInvertedTimestamp(t *testing.T) {
	ts := int64(1000000)
	inv := invertedTimestamp(ts)
	back := timestampFromInverted(inv)
	if back != ts {
		t.Fatalf("round-trip: got %d, want %d", back, ts)
	}

	// Newest timestamp (MaxInt64) should sort first (smallest inverted).
	newest := invertedTimestamp(math.MaxInt64)
	oldest := invertedTimestamp(0)
	if newest >= oldest {
		t.Fatal("newest timestamp should have smaller inverted value")
	}
}

func TestEncodeDecodeCellKeyRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		rowKey    []byte
		family    string
		qualifier []byte
		ts        int64
	}{
		{"simple", []byte("row1"), "cf", []byte("q"), 1000000},
		{"empty qualifier", []byte("row1"), "cf", []byte{}, 5000000},
		{"binary key", []byte{0x00, 0x01, 0xFF}, "fam", []byte("col"), 0},
		{"max timestamp", []byte("row"), "f", []byte("q"), math.MaxInt64},
		{"long family", []byte("rk"), "abcdefghijklmnopqrstuvwxyz", []byte("qual"), 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := EncodeCellKey(tt.rowKey, tt.family, tt.qualifier, tt.ts)
			rk, fam, qual, ts, ok := DecodeCellKey(key)
			if !ok {
				t.Fatal("DecodeCellKey returned false")
			}
			if !bytes.Equal(rk, tt.rowKey) {
				t.Fatalf("rowKey: got %v, want %v", rk, tt.rowKey)
			}
			if fam != tt.family {
				t.Fatalf("family: got %q, want %q", fam, tt.family)
			}
			if !bytes.Equal(qual, tt.qualifier) {
				t.Fatalf("qualifier: got %v, want %v", qual, tt.qualifier)
			}
			if ts != tt.ts {
				t.Fatalf("timestamp: got %d, want %d", ts, tt.ts)
			}
		})
	}
}

func TestEncodeCellKeyDeterministic(t *testing.T) {
	key1 := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	key2 := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if !bytes.Equal(key1, key2) {
		t.Fatal("encoding should be deterministic")
	}
}

func TestDecodeCellKeyErrors(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"too short for row len", []byte{0x00}},
		{"missing sentinel after row", func() []byte {
			b := make([]byte, 3)
			binary.BigEndian.PutUint16(b, 1)
			b[2] = 'x'
			return b
		}()},
		{"truncated after row", func() []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint16(b, 2)
			b[2] = 'a'
			b[3] = 'b'
			return b
		}()},
		{"missing qualifier", []byte{0x01, 0x00, 'a', 0x00, 0x01, 'c', 0x00, 0x00, 0x01, 'q', 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, ok := DecodeCellKey(tt.key)
			if ok {
				t.Fatal("expected false for invalid key")
			}
		})
	}
}

func TestRowPrefixScanOrder(t *testing.T) {
	rowKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	keys := make([][]byte, 0, 3)
	for _, rk := range rowKeys {
		keys = append(keys, EncodeCellKey(rk, "cf", []byte("q"), 1000))
	}

	for i := 0; i < len(keys)-1; i++ {
		if bytes.Compare(keys[i], keys[i+1]) >= 0 {
			t.Fatal("keys should be sorted by row key")
		}
	}
}

func TestTimestampSortOrder(t *testing.T) {
	rowKey := []byte("row")
	family := "cf"
	qual := []byte("q")

	oldKey := EncodeCellKey(rowKey, family, qual, 100)
	newKey := EncodeCellKey(rowKey, family, qual, 200)

	// Newer timestamps should sort first (smaller inverted value).
	if bytes.Compare(newKey, oldKey) >= 0 {
		t.Fatal("newer timestamp should sort before older timestamp")
	}
}

func TestRowEndKey(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	end := rowEndKey(rp)

	if len(end) != len(rp)+1 {
		t.Fatalf("end key length: got %d, want %d", len(end), len(rp)+1)
	}
	if end[len(rp)] != 0xFF {
		t.Fatal("end key should end with 0xFF")
	}

	// Every cell key in "row" should be < end.
	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell key should be less than end key")
	}

	// A key in "row2" should NOT be less than end.
	otherKey := EncodeCellKey([]byte("row2"), "cf", []byte("q"), 100)
	if bytes.Compare(otherKey, end) <= 0 {
		t.Fatal("row2 key should not be within row end key")
	}
}

func TestFamilyEndKey(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	fp := encodeFamilyPrefix(rp, "cf")
	end := familyEndKey(fp)

	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell key in cf should be less than cf end key")
	}

	otherKey := EncodeCellKey([]byte("row"), "cf2", []byte("q"), 100)
	if bytes.Compare(otherKey, end) <= 0 {
		t.Fatal("cell key in cf2 should not be within cf end key")
	}
}

func TestColumnEndKey(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	fp := encodeFamilyPrefix(rp, "cf")
	cp := encodeColumnPrefix(fp, []byte("q"))
	end := columnEndKey(cp)

	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell key in cf:q should be less than column end key")
	}

	otherKey := EncodeCellKey([]byte("row"), "cf", []byte("q2"), 100)
	if bytes.Compare(otherKey, end) <= 0 {
		t.Fatal("cell key in cf:q2 should not be within cf:q end key")
	}
}

func TestRowKeyRangeBounds(t *testing.T) {
	start, end := rowKeyRangeBounds([]byte("row"))

	// start should be the row prefix.
	expectedStart := encodeRowPrefix([]byte("row"))
	if !bytes.Equal(start, expectedStart) {
		t.Fatalf("start: got %v, want %v", start, expectedStart)
	}

	// end should have 0xFF suffix.
	if end[len(expectedStart)] != 0xFF {
		t.Fatal("end should end with 0xFF")
	}

	// Cell in "row" should be in bounds.
	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, start) < 0 || bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell in row should be within bounds")
	}
}

func TestRowKeyFamilyBounds(t *testing.T) {
	start, end := rowKeyFamilyBounds([]byte("row"), "cf")

	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, start) < 0 || bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell in row/cf should be within bounds")
	}

	otherKey := EncodeCellKey([]byte("row"), "cf2", []byte("q"), 100)
	if bytes.Compare(otherKey, start) >= 0 && bytes.Compare(otherKey, end) < 0 {
		t.Fatal("cell in row/cf2 should NOT be within cf bounds")
	}
}

func TestRowKeyColumnBounds(t *testing.T) {
	start, end := rowKeyColumnBounds([]byte("row"), "cf", []byte("q"))

	cellKey := EncodeCellKey([]byte("row"), "cf", []byte("q"), 100)
	if bytes.Compare(cellKey, start) < 0 || bytes.Compare(cellKey, end) >= 0 {
		t.Fatal("cell in row/cf:q should be within bounds")
	}

	otherKey := EncodeCellKey([]byte("row"), "cf", []byte("q2"), 100)
	if bytes.Compare(otherKey, start) >= 0 && bytes.Compare(otherKey, end) < 0 {
		t.Fatal("cell in row/cf:q2 should NOT be within cf:q bounds")
	}
}

func TestEncodeTimestampRangeBounds(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	fp := encodeFamilyPrefix(rp, "cf")
	cp := encodeColumnPrefix(fp, []byte("q"))

	start, end := encodeTimestampRangeBounds(cp, 100, 200)

	// Cell at ts=150 should be in bounds.
	cellIn := EncodeCellKey([]byte("row"), "cf", []byte("q"), 150)
	if bytes.Compare(cellIn, start) < 0 || bytes.Compare(cellIn, end) >= 0 {
		t.Fatal("cell at ts=150 should be within [100, 200) bounds")
	}

	// Cell at ts=50 should NOT be in bounds (less than start).
	cellBefore := EncodeCellKey([]byte("row"), "cf", []byte("q"), 50)
	if bytes.Compare(cellBefore, start) >= 0 && bytes.Compare(cellBefore, end) < 0 {
		t.Fatal("cell at ts=50 should NOT be within [100, 200) bounds")
	}

	// Cell at ts=200 should NOT be in bounds (end is exclusive).
	cellAfter := EncodeCellKey([]byte("row"), "cf", []byte("q"), 200)
	if bytes.Compare(cellAfter, start) >= 0 && bytes.Compare(cellAfter, end) < 0 {
		t.Fatal("cell at ts=200 should NOT be within [100, 200) bounds")
	}
}

func TestKeyHasRowPrefix(t *testing.T) {
	key := EncodeCellKey([]byte("row1"), "cf", []byte("q"), 100)

	if !keyHasRowPrefix(key, []byte("row1")) {
		t.Fatal("key should have row1 prefix")
	}
	if keyHasRowPrefix(key, []byte("row")) {
		t.Fatal("key should not have row prefix (partial match)")
	}
	if keyHasRowPrefix(key, []byte("row2")) {
		t.Fatal("key should not have row2 prefix")
	}
}

func TestEncodeRowPrefix(t *testing.T) {
	rp := encodeRowPrefix([]byte("hello"))
	// "hello" has no null bytes, so escaped form = "hello", + 0x00 0x00 = 7 bytes.
	if len(rp) != len("hello")+2 {
		t.Fatalf("prefix length: got %d, want %d", len(rp), len("hello")+2)
	}
	// Row key bytes should match input.
	for i := 0; i < len("hello"); i++ {
		if rp[i] != "hello"[i] {
			t.Fatalf("row key byte %d: got %02x, want %02x", i, rp[i], "hello"[i])
		}
	}
	// Terminator should be 0x00 0x00.
	if rp[len("hello")] != 0x00 || rp[len("hello")+1] != 0x00 {
		t.Fatal("expected 0x00 0x00 terminator")
	}
}

func TestEncodeFamilyPrefix(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	fp := encodeFamilyPrefix(rp, "cf")
	if len(fp) != len(rp)+1+2+1 {
		t.Fatalf("family prefix length mismatch")
	}
}

func TestEncodeColumnPrefix(t *testing.T) {
	rp := encodeRowPrefix([]byte("row"))
	fp := encodeFamilyPrefix(rp, "cf")
	cp := encodeColumnPrefix(fp, []byte("qual"))
	if len(cp) != len(fp)+2+4+1 {
		t.Fatalf("column prefix length mismatch")
	}
}
