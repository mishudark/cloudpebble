.PHONY: proto test test-race bench build lint clean fuzz

PROTO_DIR := proto
BIGTABLEPB_DIR := pkg/bigtable/bigtablepb
PROTOC := protoc
GOOGLEAPIS_DIR ?= $(HOME)/src/github.com/google/googleapis

proto:
	@mkdir -p $(BIGTABLEPB_DIR)
	@rm -f $(BIGTABLEPB_DIR)/*.pb.go
	@mkdir -p /tmp/cloudpebble-proto-gen
	$(PROTOC) \
	  --proto_path=$(PROTO_DIR) \
	  --proto_path=$(GOOGLEAPIS_DIR) \
	  --go_out=paths=source_relative:/tmp/cloudpebble-proto-gen \
	  --go-grpc_out=paths=source_relative:/tmp/cloudpebble-proto-gen \
	  $$(find $(PROTO_DIR) -name "*.proto")
	@find /tmp/cloudpebble-proto-gen -name "*.pb.go" -exec mv {} $(BIGTABLEPB_DIR)/ \;
	@rm -rf /tmp/cloudpebble-proto-gen
	@echo "Protos generated in $(BIGTABLEPB_DIR)"

test:
	go test -v -count=1 ./pkg/...

test-race:
	go test -v -race -count=1 ./pkg/...

bench:
	go test -bench=. -benchmem -benchtime=3s ./pkg/...

build:
	go build ./cmd/...

lint:
	golangci-lint run

fuzz:
	go test -fuzz=Fuzz -fuzztime=30s ./pkg/...

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -f *.test
	rm -f coverage.out
	rm -rf /tmp/cloudpebble-proto-gen
