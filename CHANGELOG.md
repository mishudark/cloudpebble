# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
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
- Replaced Prometheus metrics with OpenTelemetry observable counters
- Pebble dependency uses pseudo-version with documented replace directive
- README updated to use non-deprecated gRPC insecure credentials
- Manifest writes are now atomic (versioned manifest written first)
- `fmt.Fprintf(os.Stderr, ...)` replaced with structured logging

### Fixed
- Race condition in `Close()` — now waits for background goroutines
- Manifest write ordering — versioned manifest written before current pointer
