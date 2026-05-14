package engine

import (
	"sync/atomic"
	"time"
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

	SyncCalls        atomic.Int64
	SyncFailures     atomic.Int64

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
