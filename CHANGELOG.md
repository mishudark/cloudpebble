# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Parallel SST downloads during recovery (8-way errgroup)
- Parallel SST uploads during Sync (8-way errgroup)
- Parallel WAL deletes in GC (8-way errgroup)
- Dedicated flush worker goroutine in WAL manager (avoids goroutine-per-flush)
- sync.Pool for WAL merge batch buffers
- `PutReader` and `GetReader` streaming methods on Store interface
- GitHub Actions CI workflow with test, lint, and vet jobs
- golangci-lint configuration
- Dockerfile for pebble-bigtable server
- `make test`, `make test-race`, `make lint`, `make bench`, `make build` targets
- `.gitignore`, `LICENSE`, `CONTRIBUTING.md`
- Structured logging via `log/slog` in engine options
- Error channel for background goroutine errors (`Engine.Errors()`)
- Graceful shutdown with WaitGroup
- Streaming store interface (`PutReader`, `GetReader`)
- Health and readiness checks (`Engine.Health()`, `Engine.Ready()`)
- OpenTelemetry metrics export (`Metrics.RegisterOpenTelemetry()`)
- Context propagation to `Open(ctx, opts)`

### Changed
- Parallel SST transfers in Sync and recovery (8-way concurrent)
- Local Pebble apply overlapped with GCS WAL upload in batching path
- EncodeCellKey reduced from 4 allocations to 1
- Eliminated dual file read in Sync (hash computed during upload pass)
- WAL GC skips sorting (only recovery needs ordered listing)
- bytes.Compare replaces strings.Compare in range filters
- Per-batch timestamp caching in mutations (1 syscall instead of N)
- Per-row lock map cleaned up on release (prevents unbounded growth)
- Local store List uses os.ReadDir fast path for flat-directory prefixes
- Replaced Prometheus metrics with OpenTelemetry observable counters
- Pebble dependency uses pseudo-version with documented replace directive
- README updated to use non-deprecated gRPC insecure credentials
- Manifest writes are now atomic (versioned manifest written first)
- `fmt.Fprintf(os.Stderr, ...)` replaced with structured logging

### Fixed
- Race condition in `Close()` — now waits for background goroutines
- Manifest write ordering — versioned manifest written before current pointer
- Continuation chunks in appendCellChunks now copy value slices instead of referencing iterator buffer
