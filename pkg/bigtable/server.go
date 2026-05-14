package bigtable

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"github.com/mishudark/cloudpebble/pkg/engine"
	"github.com/mishudark/cloudpebble/pkg/objstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the Bigtable gRPC service backed by CloudPebble engines.
// Each Bigtable table maps to a cloudpebble namespace, creating a separate
// Pebble DB + object storage backend per table.
type Server struct {
	bigtablepb.UnimplementedBigtableServer

	mu    sync.RWMutex
	dir   string          // base directory for table data
	store objstore.Store  // object storage backend

	tables map[string]*tableState // table name → engine + config

	// Namespace suffix for table names. Table "foo" becomes namespace "bigtable/foo".
	nsPrefix string

	// engineOverrides are merged into engine.Open options in getEngine.
	// Used by tests to disable WAL batching and speed up engine Open.
	engineOverrides engine.Options
}

type tableState struct {
	engine *engine.Engine
}

// NewServer creates a new Bigtable server.
// dir is the local directory for Pebble data. store is the object storage
// backend for durability.
func NewServer(dir string, store objstore.Store) *Server {
	return &Server{
		dir:    dir,
		store:  store,
		tables: make(map[string]*tableState),
	}
}

// Close shuts down the server, closing all open engines.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, ts := range s.tables {
		if err := ts.engine.Close(); err != nil {
			return fmt.Errorf("closing table %q: %w", name, err)
		}
	}
	s.tables = make(map[string]*tableState)
	return nil
}

// tableNamespace converts a Bigtable table name to a cloudpebble namespace.
func (s *Server) tableNamespace(tableName string) string {
	if s.nsPrefix != "" {
		return s.nsPrefix + "/" + tableName
	}
	return "bigtable/" + tableName
}

// getEngine returns the engine for a table, opening it if necessary.
func (s *Server) getEngine(ctx context.Context, tableName string) (*engine.Engine, error) {
	s.mu.RLock()
	ts, ok := s.tables[tableName]
	s.mu.RUnlock()
	if ok && ts.engine != nil {
		return ts.engine, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock.
	ts, ok = s.tables[tableName]
	if ok && ts.engine != nil {
		return ts.engine, nil
	}

	opts := engine.Options{
		Dir:       s.dir + "/" + tableName,
		Store:     s.store,
		Namespace: s.tableNamespace(tableName),
	}
	if s.engineOverrides.BatchWindow != 0 {
		opts.BatchWindow = s.engineOverrides.BatchWindow
	}
	eng, err := engine.Open(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("opening table %q: %w", tableName, err)
	}

	s.tables[tableName] = &tableState{engine: eng}
	return eng, nil
}

// db returns the raw Pebble DB for a table.
func (s *Server) db(ctx context.Context, tableName string) (*pebble.DB, error) {
	eng, err := s.getEngine(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return eng.DB(), nil
}

// ---------------------------------------------------------------------------
// GetClientConfiguration
// ---------------------------------------------------------------------------

func (s *Server) GetClientConfiguration(ctx context.Context, req *bigtablepb.GetClientConfigurationRequest) (*bigtablepb.ClientConfiguration, error) {
	return &bigtablepb.ClientConfiguration{}, nil
}

// ---------------------------------------------------------------------------
// PingAndWarm
// ---------------------------------------------------------------------------

func (s *Server) PingAndWarm(ctx context.Context, req *bigtablepb.PingAndWarmRequest) (*bigtablepb.PingAndWarmResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	// Touch the Pebble DB to warm caches.
	eng, err := s.getEngine(ctx, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "opening table: %v", err)
	}
	_ = eng.Metrics()
	return &bigtablepb.PingAndWarmResponse{}, nil
}

// ---------------------------------------------------------------------------
// Session RPCs (OpenTable, OpenAuthorizedView, OpenMaterializedView)
// ---------------------------------------------------------------------------

func (s *Server) OpenTable(stream grpc.BidiStreamingServer[bigtablepb.SessionRequest, bigtablepb.SessionResponse]) error {
	return s.handleSession(stream)
}

func (s *Server) OpenAuthorizedView(stream grpc.BidiStreamingServer[bigtablepb.SessionRequest, bigtablepb.SessionResponse]) error {
	return status.Error(codes.Unimplemented, "authorized views not supported")
}

func (s *Server) OpenMaterializedView(stream grpc.BidiStreamingServer[bigtablepb.SessionRequest, bigtablepb.SessionResponse]) error {
	return status.Error(codes.Unimplemented, "materialized views not supported")
}

// ---------------------------------------------------------------------------
// Stubs for unimplemented Phase 1 RPCs
// ---------------------------------------------------------------------------

func (s *Server) GenerateInitialChangeStreamPartitions(req *bigtablepb.GenerateInitialChangeStreamPartitionsRequest, stream grpc.ServerStreamingServer[bigtablepb.GenerateInitialChangeStreamPartitionsResponse]) error {
	return status.Error(codes.Unimplemented, "change streams not supported")
}

func (s *Server) ReadChangeStream(req *bigtablepb.ReadChangeStreamRequest, stream grpc.ServerStreamingServer[bigtablepb.ReadChangeStreamResponse]) error {
	return status.Error(codes.Unimplemented, "change streams not supported")
}

func (s *Server) PrepareQuery(ctx context.Context, req *bigtablepb.PrepareQueryRequest) (*bigtablepb.PrepareQueryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SQL queries not supported")
}

func (s *Server) ExecuteQuery(req *bigtablepb.ExecuteQueryRequest, stream grpc.ServerStreamingServer[bigtablepb.ExecuteQueryResponse]) error {
	return status.Error(codes.Unimplemented, "SQL queries not supported")
}

// ensure BigtableServer is satisfied.
var _ bigtablepb.BigtableServer = (*Server)(nil)
