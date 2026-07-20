/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/node/handlers"
)

// channelSession is a mock aether.Session backed by Go channels.
type channelSession struct {
	send   chan []byte
	recv   chan []byte
	nodeID aether.NodeID
	closed bool
	mu     sync.Mutex
}

func (s *channelSession) Send(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.send <- payload:
		return nil
	}
}

func (s *channelSession) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data, ok := <-s.recv:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	}
}

func (s *channelSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.send)
	}
	return nil
}

func (s *channelSession) RemoteAddr() net.Addr          { return &net.UDPAddr{} }
func (s *channelSession) RemoteNodeID() aether.NodeID   { return s.nodeID }
func (s *channelSession) NetConn() net.Conn             { return nil }
func (s *channelSession) Protocol() aether.Protocol     { return aether.ProtoNoise }
func (s *channelSession) OnClose(_ func())              {}

// newSessionPair creates a linked pair of sessions (client ↔ server).
func newSessionPair() (*channelSession, *channelSession) {
	c2s := make(chan []byte, 10)
	s2c := make(chan []byte, 10)
	client := &channelSession{send: c2s, recv: s2c, nodeID: "client-node"}
	server := &channelSession{send: s2c, recv: c2s, nodeID: "server-node"}
	return client, server
}

// sendRequest marshals and sends a binary RPC request.
func sendRequest(t *testing.T, ctx context.Context, session *channelSession, req *pb.RPCRequest) {
	t.Helper()
	data, err := pb.MarshalRequest(req)
	if err != nil {
		t.Fatalf("MarshalRequest: %v", err)
	}
	if err := session.Send(ctx, data); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

// recvResponse reads and unmarshals a binary RPC response.
func recvResponse(t *testing.T, ctx context.Context, session *channelSession) *pb.RPCResponse {
	t.Helper()
	data, err := session.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	resp, err := pb.UnmarshalResponse(data)
	if err != nil {
		t.Fatalf("UnmarshalResponse: %v", err)
	}
	return resp
}

// echoRPCHandler is a test handler that echoes the payload back.
type echoRPCHandler struct{}

func (h *echoRPCHandler) Name() string                      { return "test.echo" }
func (h *echoRPCHandler) Role() string                      { return "test" }
func (h *echoRPCHandler) RequiresAuth() bool                { return false }
func (h *echoRPCHandler) AllowedAuthTypes() []string        { return nil }
func (h *echoRPCHandler) Scopes() []string                  { return nil }
func (h *echoRPCHandler) TenantScope() handlers.TenantScope { return "" }
func (h *echoRPCHandler) AllowedTenants() []string          { return nil }

func (h *echoRPCHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return &handlers.RPCResponse{
		Success: true,
		Payload: req.Payload,
	}, nil
}

func TestBinaryRPCPingHandler(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetEnabledRoles([]string{"system"})
	_ = registry.RegisterRPC(&PingRPCHandler{})

	server := NewRPCServer(registry)
	client, serverSess := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go server.ServeSession(ctx, serverSess)

	// Send ping request
	pingPayload, _ := json.Marshal(map[string]string{"message": "hello"})
	sendRequest(t, ctx, client, &pb.RPCRequest{
		Id:      "ping-1",
		Handler: "ping",
		Payload: pingPayload,
	})

	resp := recvResponse(t, ctx, client)
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	if resp.Id != "ping-1" {
		t.Fatalf("expected ID ping-1, got %s", resp.Id)
	}

	// Verify response payload contains "pong"
	var pingResp struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(resp.Payload, &pingResp); err != nil {
		t.Fatalf("unmarshal ping response: %v", err)
	}
	if pingResp.Message != "pong: hello" {
		t.Fatalf("expected 'pong: hello', got %q", pingResp.Message)
	}
}

func TestBinaryRPCRegistryDispatch(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetEnabledRoles([]string{"test"})
	_ = registry.RegisterRPC(&echoRPCHandler{})

	server := NewRPCServer(registry)
	client, serverSess := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go server.ServeSession(ctx, serverSess)

	payload := []byte(`{"data":"test-payload"}`)
	sendRequest(t, ctx, client, &pb.RPCRequest{
		Id:      "echo-1",
		Handler: "test.echo",
		Payload: payload,
	})

	resp := recvResponse(t, ctx, client)
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	if resp.Id != "echo-1" {
		t.Fatalf("expected ID echo-1, got %s", resp.Id)
	}
	if string(resp.Payload) != string(payload) {
		t.Fatalf("expected payload %q, got %q", payload, resp.Payload)
	}
}

func TestBinaryRPCUnknownHandler(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	server := NewRPCServer(registry)
	client, serverSess := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go server.ServeSession(ctx, serverSess)

	sendRequest(t, ctx, client, &pb.RPCRequest{
		Id:      "bad-1",
		Handler: "nonexistent.method",
		Payload: nil,
	})

	resp := recvResponse(t, ctx, client)
	if resp.Success {
		t.Fatal("expected failure for unknown handler")
	}
	if resp.Error == "" {
		t.Fatal("expected error message for unknown handler")
	}
}

func TestBinaryRPCMultipleRequests(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetEnabledRoles([]string{"test"})
	_ = registry.RegisterRPC(&echoRPCHandler{})

	server := NewRPCServer(registry)
	client, serverSess := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go server.ServeSession(ctx, serverSess)

	// Send 5 sequential requests on the same session
	for i := 0; i < 5; i++ {
		payload := []byte(`{"n":` + string(rune('0'+i)) + `}`)
		sendRequest(t, ctx, client, &pb.RPCRequest{
			Id:      "multi-" + string(rune('0'+i)),
			Handler: "test.echo",
			Payload: payload,
		})

		resp := recvResponse(t, ctx, client)
		if !resp.Success {
			t.Fatalf("request %d: expected success, got error: %s", i, resp.Error)
		}
	}
}

func TestBinaryRPCSessionClose(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	server := NewRPCServer(registry)
	_, serverSess := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Close the client side (server's recv channel) to simulate disconnect
	close(serverSess.recv)

	// ServeSession should return nil on EOF (clean close)
	err := server.ServeSession(ctx, serverSess)
	if err != nil {
		t.Fatalf("expected nil on clean close, got: %v", err)
	}
}
