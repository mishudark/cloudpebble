.PHONY: proto
PROTO_DIR := proto
BIGTABLEPB_DIR := pkg/bigtable/bigtablepb
PROTOC := protoc

proto:
	@mkdir -p $(BIGTABLEPB_DIR)
	@rm -f $(BIGTABLEPB_DIR)/*.pb.go
	@mkdir -p /tmp/cloudpebble-proto-gen
	$(PROTOC) \
	  --proto_path=$(PROTO_DIR) \
	  --proto_path=/home/thinkpad/src/github.com/google/googleapis \
	  --go_out=paths=source_relative:/tmp/cloudpebble-proto-gen \
	  --go-grpc_out=paths=source_relative:/tmp/cloudpebble-proto-gen \
	  $$(find $(PROTO_DIR) -name "*.proto")
	@find /tmp/cloudpebble-proto-gen -name "*.pb.go" -exec mv {} $(BIGTABLEPB_DIR)/ \;
	@rm -rf /tmp/cloudpebble-proto-gen
	@echo "Protos generated in $(BIGTABLEPB_DIR)"
