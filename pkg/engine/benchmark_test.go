package engine_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/engine"
)

// TestBloomFiltersEnabledByDefault verifies the engine enables Bloom filters
// on its Pebble instance: negative lookups against flushed SSTables must be
// short-circuited by the filter (recorded as filter hits) rather than reading
// data blocks.
func TestBloomFiltersEnabledByDefault(t *testing.T) {
	e := newTestEngine(t, "bloom", func(o *engine.Options) {
		o.ColdMissThreshold = 1 << 30 // don't trigger cold-miss recovery on misses
	})
	db := e.DB()

	// Write even-numbered keys so odd-numbered misses fall strictly inside
	// the SSTable key range: Pebble can only skip them via the filter, not
	// via table bounds.
	for i := range 2000 {
		key := fmt.Appendf(nil, "key%08d", i*2)
		if err := db.Set(key, []byte("value"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := range 200 {
		missing := fmt.Appendf(nil, "key%08d", i*2+1)
		if _, err := e.Get(missing); err != pebble.ErrNotFound {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}

	m := db.Metrics()
	if m.Filter.Hits == 0 {
		t.Fatal("expected Bloom filter hits on negative lookups; filters are not active")
	}
}

// BenchmarkEngineGetMiss measures negative lookups through the engine with
// Bloom filters enabled (the default), against data resident in SSTables.
func BenchmarkEngineGetMiss(b *testing.B) {
	e := newTestEngine(b, "bloom-bench", func(o *engine.Options) {
		o.ColdMissThreshold = 1 << 30
	})
	db := e.DB()
	populateAndFlush(b, db)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		missing := fmt.Appendf(nil, "key%08d", (i%49999)*2+1)
		i++
		if _, err := e.Get(missing); err != pebble.ErrNotFound {
			b.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
}

// BenchmarkEngineGetHit measures positive lookups through the engine with
// Bloom filters enabled.
func BenchmarkEngineGetHit(b *testing.B) {
	e := newTestEngine(b, "bloom-bench", func(o *engine.Options) {
		o.ColdMissThreshold = 1 << 30
	})
	db := e.DB()
	populateAndFlush(b, db)

	key := fmt.Appendf(nil, "key%08d", 24680)
	b.ResetTimer()
	for b.Loop() {
		if _, err := e.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngineGetMissNoFilter is the baseline: negative lookups through
// the engine with filters explicitly disabled, showing what the default
// Bloom filters save per miss.
func BenchmarkEngineGetMissNoFilter(b *testing.B) {
	e := newTestEngine(b, "bloom-bench", func(o *engine.Options) {
		o.ColdMissThreshold = 1 << 30
		for i := range o.PebbleOptions.Levels {
			o.PebbleOptions.Levels[i].TableFilterPolicy = func() pebble.TableFilterPolicy {
				return pebble.NoFilterPolicy
			}
		}
	})
	db := e.DB()
	populateAndFlush(b, db)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		missing := fmt.Appendf(nil, "key%08d", (i%49999)*2+1)
		i++
		if _, err := e.Get(missing); err != pebble.ErrNotFound {
			b.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
}

// populateAndFlush writes 50k even-numbered keys across several flushed
// SSTables so point lookups must consult on-disk tables rather than the
// memtable, and odd-numbered misses fall inside table key ranges.
func populateAndFlush(b *testing.B, db *pebble.DB) {
	b.Helper()
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 10000; i++ {
			key := fmt.Appendf(nil, "key%08d", (batch*10000+i)*2)
			if err := db.Set(key, []byte("value"), pebble.NoSync); err != nil {
				b.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}
