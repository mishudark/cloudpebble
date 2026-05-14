package bigtable

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"github.com/mishudark/cloudpebble/pkg/objstore/local"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewServer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	objDir := filepath.Join(t.TempDir(), "objstore")

	store, err := local.New(objDir)
	if err != nil {
		t.Fatal(err)
	}

	s := NewServer(dir, store)
	if s == nil {
		t.Fatal("expected non-nil server")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

func TestServerCloseEmpty(t *testing.T) {
	s := NewServer(t.TempDir(), nil)
	// Close with no tables opened.
	if err := s.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

func TestServerCloseMultipleTables(t *testing.T) {
	s := newTestServer(t)

	// Open a couple tables.
	ctx := context.Background()
	eng1, err := s.getEngine(ctx, "table1")
	if err != nil {
		t.Fatal(err)
	}
	eng2, err := s.getEngine(ctx, "table2")
	if err != nil {
		t.Fatal(err)
	}

	_ = eng1
	_ = eng2

	if err := s.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

func TestGetClientConfiguration(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	resp, err := s.GetClientConfiguration(ctx, &bigtablepb.GetClientConfigurationRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestPingAndWarm(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Open the table first, then ping it.
	table := "projects/p/instances/i/tables/t"
	_, err := s.getEngine(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	req := &bigtablepb.PingAndWarmRequest{
		Name: table,
	}
	resp, err := s.PingAndWarm(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestPingAndWarmMissingName(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	req := &bigtablepb.PingAndWarmRequest{}
	_, err := s.PingAndWarm(ctx, req)
	if err == nil {
		t.Fatal("expected error for missing name")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestPingAndWarmOpensTable(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// PingAndWarm auto-creates the engine if it doesn't exist.
	req := &bigtablepb.PingAndWarmRequest{
		Name: "new-table",
	}
	_, err := s.PingAndWarm(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTableNamespace(t *testing.T) {
	s := NewServer(t.TempDir(), nil)
	ns := s.tableNamespace("projects/p/instances/i/tables/mytable")
	if ns != "bigtable/projects/p/instances/i/tables/mytable" {
		t.Fatalf("namespace: got %q", ns)
	}
}

func TestGetEngineCreatesEngine(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	eng, err := s.getEngine(ctx, "test-table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eng == nil {
		t.Fatal("expected non-nil engine")
	}

	// Getting the same table should return same engine.
	eng2, err := s.getEngine(ctx, "test-table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eng != eng2 {
		t.Fatal("expected same engine for same table")
	}
}

func TestDB(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	db, err := s.db(ctx, "test-table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil db")
	}
}

func TestUnimplementedMethods(t *testing.T) {
	s := newTestServer(t)

	ctx := context.Background()

	err := s.OpenAuthorizedView(nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}

	err = s.OpenMaterializedView(nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}

	_, err = s.PrepareQuery(ctx, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}
}
