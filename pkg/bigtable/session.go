package bigtable

import (
	"context"
	"fmt"
	"io"

	"github.com/mishudark/cloudpebble/pkg/bigtable/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// handleSession manages an OpenTable bidirectional stream session.
func (s *Server) handleSession(stream grpc.BidiStreamingServer[bigtablepb.SessionRequest, bigtablepb.SessionResponse]) error {
	var tableName string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := req.Payload.(type) {
		case *bigtablepb.SessionRequest_OpenSession:
			openReq := &bigtablepb.OpenTableRequest{}
			if err := proto.Unmarshal(payload.OpenSession.GetPayload(), openReq); err == nil {
				tableName = openReq.GetTableName()
			}
			if err := stream.Send(&bigtablepb.SessionResponse{
				Payload: &bigtablepb.SessionResponse_OpenSession{
					OpenSession: &bigtablepb.OpenSessionResponse{},
				},
			}); err != nil {
				return err
			}
			continue

		case *bigtablepb.SessionRequest_CloseSession:
			return nil
		}

		vrpc := req.GetVirtualRpc()
		if vrpc == nil {
			continue
		}

		if err := s.dispatchVRPC(stream, tableName, vrpc); err != nil {
			stream.Send(&bigtablepb.SessionResponse{
				Payload: &bigtablepb.SessionResponse_Error{
					Error: &bigtablepb.ErrorResponse{RpcId: vrpc.GetRpcId()},
				},
			})
		}
	}
}

// dispatchVRPC routes a vRPC request to the appropriate handler.
func (s *Server) dispatchVRPC(stream grpc.BidiStreamingServer[bigtablepb.SessionRequest, bigtablepb.SessionResponse], tableName string, vrpc *bigtablepb.VirtualRpcRequest) error {
	payload := vrpc.GetPayload()
	if len(payload) == 0 {
		return fmt.Errorf("empty vRPC payload")
	}
	if tableName == "" {
		return fmt.Errorf("no table session established")
	}

	rpcID := vrpc.GetRpcId()
	ctx := stream.Context()

	var readReq bigtablepb.ReadRowsRequest
	if err := proto.Unmarshal(payload, &readReq); err == nil && readReq.TableName != "" {
		readReq.TableName = tableName
		w := newSessionReadRowsStream(stream, rpcID, ctx)
		return s.ReadRows(&readReq, w)
	}

	var checkReq bigtablepb.CheckAndMutateRowRequest
	if err := proto.Unmarshal(payload, &checkReq); err == nil && len(checkReq.GetRowKey()) > 0 {
		resp, err := s.CheckAndMutateRow(ctx, &checkReq)
		if err != nil {
			return err
		}
		sr, _ := marshalSessionResponse(rpcID, resp)
		return stream.Send(sr)
	}

	var mutateReq bigtablepb.MutateRowRequest
	if err := proto.Unmarshal(payload, &mutateReq); err == nil && len(mutateReq.GetRowKey()) > 0 {
		resp, err := s.MutateRow(ctx, &mutateReq)
		if err != nil {
			return err
		}
		sr, _ := marshalSessionResponse(rpcID, resp)
		return stream.Send(sr)
	}

	var mutateRowsReq bigtablepb.MutateRowsRequest
	if err := proto.Unmarshal(payload, &mutateRowsReq); err == nil && len(mutateRowsReq.GetEntries()) > 0 {
		w := newSessionMutateRowsStream(stream, rpcID, ctx)
		return s.MutateRows(&mutateRowsReq, w)
	}

	var sampleReq bigtablepb.SampleRowKeysRequest
	if err := proto.Unmarshal(payload, &sampleReq); err == nil && sampleReq.TableName != "" {
		w := newSessionSampleRowKeysStream(stream, rpcID, ctx)
		return s.SampleRowKeys(&sampleReq, w)
	}

	var pingReq bigtablepb.PingAndWarmRequest
	if err := proto.Unmarshal(payload, &pingReq); err == nil && pingReq.GetName() != "" {
		resp, err := s.PingAndWarm(ctx, &pingReq)
		if err != nil {
			return err
		}
		sr, _ := marshalSessionResponse(rpcID, resp)
		return stream.Send(sr)
	}

	return fmt.Errorf("unsupported vRPC request type")
}

// --- stream wrappers for session-based RPCs ---

// streamWriter abstracts the Send method of the bidi session stream.
type streamWriter interface {
	Send(*bigtablepb.SessionResponse) error
}

// baseSessionStream provides grpc.ServerStream interface methods.
// All session stream wrappers embed this.
type baseSessionStream struct {
	writer streamWriter
	rpcID  int64
	ctx    context.Context
}

func (b *baseSessionStream) SetHeader(md metadata.MD) error  { return nil }
func (b *baseSessionStream) SendHeader(md metadata.MD) error { return nil }
func (b *baseSessionStream) SetTrailer(md metadata.MD)       {}
func (b *baseSessionStream) Context() context.Context        { return b.ctx }
func (b *baseSessionStream) SendMsg(m any) error             { return nil }
func (b *baseSessionStream) RecvMsg(m any) error             { return io.EOF }

func (b *baseSessionStream) sendToSession(resp proto.Message) error {
	sr, _ := marshalSessionResponse(b.rpcID, resp)
	return b.writer.Send(sr)
}

type sessionReadRowsStream struct {
	baseSessionStream
}

func newSessionReadRowsStream(writer streamWriter, rpcID int64, ctx context.Context) *sessionReadRowsStream {
	return &sessionReadRowsStream{baseSessionStream{writer: writer, rpcID: rpcID, ctx: ctx}}
}

func (s *sessionReadRowsStream) Send(resp *bigtablepb.ReadRowsResponse) error {
	return s.sendToSession(resp)
}

type sessionMutateRowsStream struct {
	baseSessionStream
}

func newSessionMutateRowsStream(writer streamWriter, rpcID int64, ctx context.Context) *sessionMutateRowsStream {
	return &sessionMutateRowsStream{baseSessionStream{writer: writer, rpcID: rpcID, ctx: ctx}}
}

func (s *sessionMutateRowsStream) Send(resp *bigtablepb.MutateRowsResponse) error {
	return s.sendToSession(resp)
}

type sessionSampleRowKeysStream struct {
	baseSessionStream
}

func newSessionSampleRowKeysStream(writer streamWriter, rpcID int64, ctx context.Context) *sessionSampleRowKeysStream {
	return &sessionSampleRowKeysStream{baseSessionStream{writer: writer, rpcID: rpcID, ctx: ctx}}
}

func (s *sessionSampleRowKeysStream) Send(resp *bigtablepb.SampleRowKeysResponse) error {
	return s.sendToSession(resp)
}

var (
	_ grpc.ServerStreamingServer[bigtablepb.ReadRowsResponse]        = (*sessionReadRowsStream)(nil)
	_ grpc.ServerStreamingServer[bigtablepb.MutateRowsResponse]      = (*sessionMutateRowsStream)(nil)
	_ grpc.ServerStreamingServer[bigtablepb.SampleRowKeysResponse]   = (*sessionSampleRowKeysStream)(nil)
)

func marshalSessionResponse(rpcID int64, payload proto.Message) (*bigtablepb.SessionResponse, error) {
	data, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &bigtablepb.SessionResponse{
		Payload: &bigtablepb.SessionResponse_VirtualRpc{
			VirtualRpc: &bigtablepb.VirtualRpcResponse{
				RpcId:   rpcID,
				Payload: data,
			},
		},
	}, nil
}
