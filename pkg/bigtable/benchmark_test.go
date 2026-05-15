package bigtable

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
)

const benchTable = "projects/p/instances/i/tables/t"

// sequentialKey returns a deterministic consecutive row key so benchmarks
// measure true ingest (new keys), not overwrite.
func sequentialKey(i int) []byte {
	return []byte(fmt.Sprintf("row_%012d", i))
}

// BenchmarkMutateRow measures single-row mutations per second.
// Every write goes through the full cloudpebble engine path:
//   engine.Apply → WAL write to object storage → Pebble apply.
func BenchmarkMutateRow(b *testing.B) {
	s := newTestServer(b)
	ctx := context.Background()
	table := benchTable

	val := make([]byte, 64)
	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		Mutations: []*bigtablepb.Mutation{
			{
				Mutation: &bigtablepb.Mutation_SetCell_{
					SetCell: &bigtablepb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("q"),
						TimestampMicros: -1,
						Value:           val,
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.RowKey = sequentialKey(i)
		if _, err := s.MutateRow(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMutateRowMultiCell writes 10 cells per row sequentially.
func BenchmarkMutateRowMultiCell(b *testing.B) {
	s := newTestServer(b)
	ctx := context.Background()
	table := benchTable

	mutations := make([]*bigtablepb.Mutation, 10)
	for i := range mutations {
		mutations[i] = &bigtablepb.Mutation{
			Mutation: &bigtablepb.Mutation_SetCell_{
				SetCell: &bigtablepb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte(fmt.Sprintf("q%03d", i)),
					TimestampMicros: -1,
					Value:           make([]byte, 64),
				},
			},
		}
	}

	req := &bigtablepb.MutateRowRequest{
		TableName: table,
		Mutations: mutations,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.RowKey = sequentialKey(i)
		if _, err := s.MutateRow(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMutateRows measures batched multi-row mutations.
// Each MutateRows call sends batchSize entries, each to a unique consecutive
// row key. The WAL batch covers all entries atomically.
func BenchmarkMutateRows(b *testing.B) {
	b.Run("batch=10", func(b *testing.B) { benchmarkMutateRows(b, 10) })
	b.Run("batch=50", func(b *testing.B) { benchmarkMutateRows(b, 50) })
	b.Run("batch=100", func(b *testing.B) { benchmarkMutateRows(b, 100) })
}

func benchmarkMutateRows(b *testing.B, batchSize int) {
	s := newTestServer(b)
	table := benchTable

	mutation := &bigtablepb.Mutation{
		Mutation: &bigtablepb.Mutation_SetCell_{
			SetCell: &bigtablepb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte("q"),
				TimestampMicros: -1,
				Value:           make([]byte, 64),
			},
		},
	}

	entries := make([]*bigtablepb.MutateRowsRequest_Entry, batchSize)
	for i := range entries {
		entries[i] = &bigtablepb.MutateRowsRequest_Entry{
			RowKey:    sequentialKey(i),
			Mutations: []*bigtablepb.Mutation{mutation},
		}
	}

	req := &bigtablepb.MutateRowsRequest{
		TableName: table,
		Entries:   entries,
	}

	stream := newMockServerStream[*bigtablepb.MutateRowsResponse]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := i * batchSize
		for j := range entries {
			entries[j].RowKey = sequentialKey(offset + j)
		}
		if err := s.MutateRows(req, stream); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadModifyWriteRow measures ReadModifyWriteRow throughput.
// Each operation reads the current value, appends, and writes — all on
// consecutive rows so each op works on a fresh cell.
func BenchmarkReadModifyWriteRow(b *testing.B) {
	s := newTestServer(b)
	ctx := context.Background()
	table := benchTable

	req := &bigtablepb.ReadModifyWriteRowRequest{
		TableName: table,
		Rules: []*bigtablepb.ReadModifyWriteRule{
			{
				FamilyName:      "cf",
				ColumnQualifier: []byte("counter"),
				Rule: &bigtablepb.ReadModifyWriteRule_IncrementAmount{
					IncrementAmount: 1,
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.RowKey = sequentialKey(i)
		if _, err := s.ReadModifyWriteRow(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadRows measures full-table scan throughput from a
// pre-populated table with 10k rows (1 cell each, 256B values).
func BenchmarkReadRows(b *testing.B) {
	s := newTestServer(b)
	table := benchTable

	eng := openTableEngine(b, s, table)
	db := eng.DB()

	for i := 0; i < 10000; i++ {
		key := EncodeCellKey(sequentialKey(i), "cf", []byte("q"), int64(i))
		if err := db.Set(key, make([]byte, 256), pebble.NoSync); err != nil {
			b.Fatal(err)
		}
	}

	req := &bigtablepb.ReadRowsRequest{
		TableName: table,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream := newMockServerStream[*bigtablepb.ReadRowsResponse]()
		if err := s.ReadRows(req, stream); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSequentialWrites measures wall-clock ingest throughput.
// Each write targets a consecutive row key so every op is a new key.
// Uses `go test -benchtime=3s` to run for a fixed duration.
func BenchmarkSequentialWrites(b *testing.B) {
	s := newTestServer(b)
	ctx := context.Background()
	table := benchTable

	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := &bigtablepb.MutateRowRequest{
			TableName: table,
			RowKey:    sequentialKey(i),
			Mutations: []*bigtablepb.Mutation{
				{
					Mutation: &bigtablepb.Mutation_SetCell_{
						SetCell: &bigtablepb.Mutation_SetCell{
							FamilyName:      "cf",
							ColumnQualifier: []byte("q"),
							TimestampMicros: int64(i),
							Value:           make([]byte, 64),
						},
					},
				},
			},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
}

// BenchmarkParallelWrites measures concurrent ingest throughput.
// Each goroutine writes to consecutive keys (no two goroutines write the
// same key) using an atomic counter.
func BenchmarkParallelWrites(b *testing.B) {
	s := newTestServer(b)
	ctx := context.Background()
	table := benchTable

	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := &bigtablepb.MutateRowRequest{
			TableName: table,
			Mutations: []*bigtablepb.Mutation{
				{
					Mutation: &bigtablepb.Mutation_SetCell_{
						SetCell: &bigtablepb.Mutation_SetCell{
							FamilyName:      "cf",
							ColumnQualifier: []byte("q"),
							TimestampMicros: -1,
							Value:           make([]byte, 64),
						},
					},
				},
			},
		}
		for pb.Next() {
			idx := int(atomic.AddInt64(&counter, 1) - 1)
			req.RowKey = sequentialKey(idx)
			if _, err := s.MutateRow(ctx, req); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
