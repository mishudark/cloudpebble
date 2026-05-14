# Contributing to CloudPebble

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests: `make test`
5. Run linter: `make lint`
6. Commit your changes (`git commit -am 'Add my feature'`)
7. Push to your fork (`git push origin feature/my-feature`)
8. Create a Pull Request

## Development Requirements

- Go 1.25+
- GNU Make
- protoc (for regenerating protobufs)

## Running Tests

```bash
# Run all unit tests
make test

# Run with race detector
make test-race

# Run benchmarks
make bench

# Run fuzz tests
go test -fuzz=Fuzz -fuzztime=30s ./pkg/...
```

## Code Style

- Follow [Effective Go](https://go.dev/doc/effective_go)
- Run `golangci-lint run` before submitting
- Keep functions focused and small
- Write tests for new functionality
- Document public APIs with godoc comments

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(engine): add cold miss recovery
fix(walcloud): prevent panic in mergeBatchSegments
docs: update README with usage examples
test(bigtable): add fuzz tests for encoding
```

## Pull Request Guidelines

- Reference related issues in the PR description
- Include tests for new functionality
- Update documentation if behavior changes
- Keep PRs focused on a single concern
