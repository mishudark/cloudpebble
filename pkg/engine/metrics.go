package engine

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

// RegisterPrometheus registers all engine metrics with the given Prometheus
// registry. The namespace and subsystem prefixes are prepended to metric names.
// Pass empty strings to use the default "cloudpebble" namespace.
func (m *Metrics) RegisterPrometheus(reg prometheus.Registerer, namespace, subsystem string) {
	if namespace == "" {
		namespace = "cloudpebble"
	}
	if subsystem == "" {
		subsystem = "engine"
	}

	fq := func(name string) string {
		return prometheus.BuildFQName(namespace, subsystem, name)
	}

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("sets_total"),
		Help: "Total number of Set operations.",
	}, func() float64 { return float64(m.Sets.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("gets_total"),
		Help: "Total number of Get operations.",
	}, func() float64 { return float64(m.Gets.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("get_hits_total"),
		Help: "Total number of cache hits.",
	}, func() float64 { return float64(m.GetHits.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("get_misses_total"),
		Help: "Total number of cache misses.",
	}, func() float64 { return float64(m.GetMisses.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("deletes_total"),
		Help: "Total number of Delete operations.",
	}, func() float64 { return float64(m.Deletes.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("cold_recoveries_total"),
		Help: "Total number of cold recovery triggers.",
	}, func() float64 { return float64(m.ColdRecoveries.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("wal_objects_written_total"),
		Help: "Total number of WAL objects written to object storage.",
	}, func() float64 { return float64(m.WALObjectsWritten.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("wal_objects_gc_total"),
		Help: "Total number of WAL objects garbage collected.",
	}, func() float64 { return float64(m.WALObjectsGCd.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("orphan_wals_gc_total"),
		Help: "Total number of orphan WALs garbage collected.",
	}, func() float64 { return float64(m.OrphanWALsGCd.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("sync_calls_total"),
		Help: "Total number of Sync calls.",
	}, func() float64 { return float64(m.SyncCalls.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("sync_failures_total"),
		Help: "Total number of Sync failures.",
	}, func() float64 { return float64(m.SyncFailures.Load()) }))

	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: fq("bytes_written_wal_total"),
		Help: "Total bytes written to WAL objects.",
	}, func() float64 { return float64(m.BytesWrittenWAL.Load()) }))
}
