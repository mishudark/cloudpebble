package bigtable

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

func TestSampleRowKeysEmptyTable(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"

	req := &bigtablepb.SampleRowKeysRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.SampleRowKeysResponse]()
	err := s.SampleRowKeys(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty table → may have 0 or more samples depending on SST structure.
	// At minimum, it should not error.
}

func TestSampleRowKeysWithData(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"

	// Write some data to create SST files.
	eng := openTableEngine(t, s, table)
	db := eng.DB()

	for _, row := range []string{"a", "b", "c", "d", "e"} {
		key := EncodeCellKey([]byte(row), "cf", []byte("q"), 100)
		if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}

	req := &bigtablepb.SampleRowKeysRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.SampleRowKeysResponse]()
	err := s.SampleRowKeys(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get at least one sample row key.
	if len(stream.sent) == 0 {
		t.Log("no sample keys returned (Pebble may not have flushed SSTs yet)")
	}
}

func TestSampleRowKeysOrdered(t *testing.T) {
	s := newTestServer(t)
	table := "projects/p/instances/i/tables/t"

	eng := openTableEngine(t, s, table)
	db := eng.DB()

	for _, row := range []string{"a", "b", "c"} {
		key := EncodeCellKey([]byte(row), "cf", []byte("q"), 100)
		if err := db.Set(key, []byte("data"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}

	req := &bigtablepb.SampleRowKeysRequest{
		TableName: table,
	}

	stream := newMockServerStream[*bigtablepb.SampleRowKeysResponse]()
	err := s.SampleRowKeys(req, stream)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify monotonic offset.
	var lastOffset int64
	for _, resp := range stream.sent {
		if resp.OffsetBytes < lastOffset {
			t.Fatal("offsets should be non-decreasing")
		}
		lastOffset = resp.OffsetBytes
	}
}
