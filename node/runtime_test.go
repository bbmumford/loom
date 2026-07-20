/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/relay"
	"github.com/bbmumford/loom/node/handlers"
)

// TestNodeIdentityPersistence verifies node identity is persisted and reloaded correctly.
func TestNodeIdentityPersistence(t *testing.T) {
	// Both calls share the same DataDir so the persisted seed is reused
	// on the second call — this is the mesh's stability guarantee across
	// restarts.
	dir := t.TempDir()
	ctx := context.Background()

	// First load should generate new identity
	id1, err := GenerateIdentity(ctx, dir)
	if err != nil {
		t.Fatalf("Failed to generate identity: %v", err)
	}
	if id1.NodeID == "" {
		t.Fatal("NodeID should not be empty")
	}
	if id1.PrivateKey == nil {
		t.Fatal("Private key should not be nil")
	}

	// Second load should return same identity
	id2, err := GenerateIdentity(ctx, dir)
	if err != nil {
		t.Fatalf("Failed to reload identity: %v", err)
	}
	if string(id2.NodeID) != string(id1.NodeID) {
		t.Errorf("NodeID mismatch: got %s, want %s", id2.NodeID, id1.NodeID)
	}

	// Verify keys match
	pub1 := id1.PrivateKey.Public().(ed25519.PublicKey)
	pub2 := id2.PrivateKey.Public().(ed25519.PublicKey)
	if string(pub1) != string(pub2) {
		t.Error("Public keys should match after reload")
	}
}

// TestConfigValidation verifies configuration validation catches errors.
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "empty config",
			config:  Config{},
			wantErr: true,
		},
		{
			name: "valid minimal config",
			config: Config{
				ServiceName: "test-service",
				DataDir:     t.TempDir(),
				NetworkKeys: []string{"testkey123456789012345678901234"}, // 32 bytes required
			},
			wantErr: false,
		},
		{
			name: "missing service name",
			config: Config{
				DataDir:     t.TempDir(),
				NetworkKeys: []string{"testkey123456789012345678901234"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCircuitBreaker verifies circuit breaker behavior.
func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	// Initial state should be closed
	if cb.State() != CircuitBreakerClosed {
		t.Error("Circuit should be closed initially")
	}

	// Successful calls should keep it closed
	err := cb.Call(func() error { return nil })
	if err != nil {
		t.Errorf("Successful call should not error: %v", err)
	}

	// Record failures
	testErr := errors.New("test error")
	_ = cb.Call(func() error { return testErr })
	_ = cb.Call(func() error { return testErr })
	if cb.State() != CircuitBreakerClosed {
		t.Error("Circuit should still be closed after 2 failures")
	}

	// Third failure should trip the breaker
	_ = cb.Call(func() error { return testErr })
	if cb.State() != CircuitBreakerOpen {
		t.Error("Circuit should be open after threshold reached")
	}

	// Calls should fail when open
	err = cb.Call(func() error { return nil })
	if err == nil {
		t.Error("Call should fail when circuit is open")
	}

	// Wait for reset time
	time.Sleep(150 * time.Millisecond)

	// Next call should succeed (half-open -> closed)
	err = cb.Call(func() error { return nil })
	if err != nil {
		t.Errorf("Call should succeed in half-open state: %v", err)
	}

	// Should be closed again
	if cb.State() != CircuitBreakerClosed {
		t.Error("Circuit should be closed after successful call in half-open")
	}
}

// TestSessionHealthMonitor verifies session health tracking.
func TestSessionHealthMonitor(t *testing.T) {
	cfg := relay.HealthConfig{
		PingInterval:   50 * time.Millisecond,
		PingTimeout:    100 * time.Millisecond,
		MaxMissedPings: 2,
		MaxSessions:    10,
	}
	monitor := NewSessionHealthMonitor(cfg)

	evicted := make(chan aether.NodeID, 1)
	monitor.SetEvictCallback(func(nodeID aether.NodeID, reason string) {
		evicted <- nodeID
	})

	// Create mock session
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	pub := priv.Public().(ed25519.PublicKey)
	nodeID, _ := aether.NewNodeID(pub)

	mockSession := &mockSession{nodeID: nodeID}
	monitor.Register(mockSession)

	// Verify count increased
	if monitor.ActiveCount() != 1 {
		t.Errorf("Expected 1 session, got %d", monitor.ActiveCount())
	}

	// Unregister
	monitor.Unregister(nodeID)
	if monitor.ActiveCount() != 0 {
		t.Errorf("Expected 0 sessions after unregister, got %d", monitor.ActiveCount())
	}
}

// TestRPCServer verifies RPC handler registration.
func TestRPCServer(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetEnabledRoles([]string{"system", "test"})
	server := NewRPCServer(registry)

	// Legacy RegisterHandler removed — all handlers use RegisterRPC

	// Register via registry
	if err := server.RegisterRPC(&PingRPCHandler{}); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}

	// Verify the ping handler works via ExecuteRPC
	pingHandler := &PingRPCHandler{}
	req, _ := json.Marshal(map[string]string{"message": "hello"})
	resp, err := pingHandler.ExecuteRPC(context.Background(), &handlers.RPCRequest{
		Handler: "ping",
		Payload: req,
	})
	if err != nil {
		t.Fatalf("Ping should not error: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Error("Ping should return a successful response")
	}
}

// TestHealthCheck verifies health check functionality.
func TestHealthCheck(t *testing.T) {
	// Create health check deps with all required callbacks
	deps := HealthCheckDeps{
		GetLeader: func() (string, error) { return "node-1", nil },
		IsLeader:  func() bool { return false },
		GetPeers:  func() ([]string, error) { return []string{"node-2"}, nil },
		// Ledger is optional
	}

	hc := NewHealthCheck(deps, 100*time.Millisecond)

	// Before starting, should be healthy (no check performed yet)
	if hc.LastResult() != nil {
		t.Errorf("Should have no error initially: %v", hc.LastResult())
	}
	if !hc.IsHealthy() {
		t.Error("Should be healthy initially")
	}

	// Start and check (let one cycle run)
	hc.Start()
	time.Sleep(150 * time.Millisecond)
	hc.Stop()

	// Should still be healthy with mock deps
	if !hc.IsHealthy() {
		t.Errorf("Should be healthy after check: %v", hc.LastResult())
	}
}

// TestIdentityFilePaths verifies identity file path construction.
func TestIdentityFilePaths(t *testing.T) {
	baseDir := "/test/data"

	// The node identity uses "node-key.json" as the filename
	privateKeyPath := filepath.Join(baseDir, "node-key.json")
	expected := filepath.Join("/test", "data", "node-key.json")
	if privateKeyPath != expected {
		t.Errorf("Unexpected private key path: got %s, want %s", privateKeyPath, expected)
	}
}

// TestNodeIdentitySignature verifies signature functionality.
func TestNodeIdentitySignature(t *testing.T) {
	identity, err := GenerateIdentity(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	// Sign a message
	message := []byte("test message")
	signature := ed25519.Sign(identity.PrivateKey, message)

	// Verify signature
	pub := identity.PrivateKey.Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, message, signature) {
		t.Error("Signature verification failed")
	}
}

// Helpers and mocks

type mockSession struct {
	nodeID aether.NodeID
}

func (m *mockSession) LocalNodeID() aether.NodeID  { return m.nodeID }
func (m *mockSession) RemoteNodeID() aether.NodeID { return m.nodeID }
func (m *mockSession) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:1234")
	return addr
}
func (m *mockSession) Send(ctx context.Context, data []byte) error {
	return nil
}
func (m *mockSession) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (m *mockSession) Close() error              { return nil }
func (m *mockSession) NetConn() net.Conn         { return nil }
func (m *mockSession) Protocol() aether.Protocol { return aether.ProtoNoise }
func (m *mockSession) OnClose(_ func())          {}

type testHandler struct{}

func (h *testHandler) Handle(ctx context.Context, method string, req json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"result":"ok"}`), nil
}

// Clean up test data directories
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
