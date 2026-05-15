package engine

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// Metrics holds operational counters and latency accumulators for the Engine.
// All counters are monotonically increasing and safe for concurrent access.
type Metrics struct {
	Sets    atomic.Int64
	Gets    atomic.Int64
	Deletes atomic.Int64

	GetMisses      atomic.Int64
	GetHits        atomic.Int64
	ColdRecoveries atomic.Int64

	WALWriteLatencyNs atomic.Int64 // cumulative nanoseconds
	ApplyLatencyNs    atomic.Int64
	SyncLatencyNs     atomic.Int64

	WALObjectsWritten atomic.Int64
	WALObjectsGCd     atomic.Int64
	OrphanWALsGCd     atomic.Int64

	SyncCalls    atomic.Int64
	SyncFailures atomic.Int64

	BytesWrittenWAL atomic.Int64 // cumulative bytes written to WAL objects
}

// Snapshot returns a point-in-time snapshot of the metrics, suitable for
// reporting or serialization.
type MetricsSnapshot struct {
	Sets                int64
	Gets                int64
	Deletes             int64
	GetMisses           int64
	GetHits             int64
	ColdRecoveries      int64
	AvgWALWriteLatency  time.Duration
	AvgApplyLatency     time.Duration
	AvgSyncLatency      time.Duration
	WALObjectsWritten   int64
	WALObjectsGCd       int64
	OrphanWALsGCd       int64
	SyncCalls           int64
	SyncFailures        int64
	BytesWrittenWAL     int64
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	s := MetricsSnapshot{
		Sets:              m.Sets.Load(),
		Gets:              m.Gets.Load(),
		Deletes:           m.Deletes.Load(),
		GetMisses:         m.GetMisses.Load(),
		GetHits:           m.GetHits.Load(),
		ColdRecoveries:    m.ColdRecoveries.Load(),
		WALObjectsWritten: m.WALObjectsWritten.Load(),
		WALObjectsGCd:     m.WALObjectsGCd.Load(),
		OrphanWALsGCd:     m.OrphanWALsGCd.Load(),
		SyncCalls:         m.SyncCalls.Load(),
		SyncFailures:      m.SyncFailures.Load(),
		BytesWrittenWAL:   m.BytesWrittenWAL.Load(),
	}
	if n := s.WALObjectsWritten; n > 0 {
		s.AvgWALWriteLatency = time.Duration(m.WALWriteLatencyNs.Load() / n)
	}
	if n := s.Sets + s.Deletes; n > 0 {
		s.AvgApplyLatency = time.Duration(m.ApplyLatencyNs.Load() / n)
	}
	if n := s.SyncCalls; n > 0 {
		s.AvgSyncLatency = time.Duration(m.SyncLatencyNs.Load() / n)
	}
	return s
}

// RegisterOpenTelemetry registers all engine metrics as OpenTelemetry
// observable counters with the given meter. The meter should be created
// from an OpenTelemetry MeterProvider with an appropriate name.
func (m *Metrics) RegisterOpenTelemetry(meter metric.Meter) error {
	instruments := []struct {
		name        string
		description string
		load        func() int64
	}{
		{"cloudpebble.engine.sets", "Total number of Set operations.", m.Sets.Load},
		{"cloudpebble.engine.gets", "Total number of Get operations.", m.Gets.Load},
		{"cloudpebble.engine.get_hits", "Total number of cache hits.", m.GetHits.Load},
		{"cloudpebble.engine.get_misses", "Total number of cache misses.", m.GetMisses.Load},
		{"cloudpebble.engine.deletes", "Total number of Delete operations.", m.Deletes.Load},
		{"cloudpebble.engine.cold_recoveries", "Total number of cold recovery triggers.", m.ColdRecoveries.Load},
		{"cloudpebble.engine.wal_objects_written", "Total number of WAL objects written to object storage.", m.WALObjectsWritten.Load},
		{"cloudpebble.engine.wal_objects_gc", "Total number of WAL objects garbage collected.", m.WALObjectsGCd.Load},
		{"cloudpebble.engine.orphan_wals_gc", "Total number of orphan WALs garbage collected.", m.OrphanWALsGCd.Load},
		{"cloudpebble.engine.sync_calls", "Total number of Sync calls.", m.SyncCalls.Load},
		{"cloudpebble.engine.sync_failures", "Total number of Sync failures.", m.SyncFailures.Load},
		{"cloudpebble.engine.bytes_written_wal", "Total bytes written to WAL objects.", m.BytesWrittenWAL.Load},
	}

	for _, inst := range instruments {
		_, err := meter.Int64ObservableCounter(
			inst.name,
			metric.WithDescription(inst.description),
			metric.WithInt64Callback(func(ctx context.Context, obs metric.Int64Observer) error {
				obs.Observe(inst.load())
				return nil
			}),
		)
		if err != nil {
			return err
		}
	}
	return nil
}
