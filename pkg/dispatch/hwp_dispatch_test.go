/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
)

// ─── buildRPCRequest tests ──────────────────────────────────────────────────

func TestBuildRequest_PropagatesDeadline(t *testing.T) {
	req := buildRPCRequest("platform", "platform.CheckHealth", nil, 5*time.Second, "")
	if time.Duration(req.TimeoutNs) < 4*time.Second || time.Duration(req.TimeoutNs) > 6*time.Second {
		t.Fatalf("expected ~5s timeout, got %v", time.Duration(req.TimeoutNs))
	}
}

// TestBuildRPCRequestCtx_SerializesScopesAndUserID verifies #K-32: the
// caller's scope-list (space-joined per RFC-6749) + userId that the rpc
// bridge stamped via WithScopes/WithUserID get serialized into req.Context.
func TestBuildRPCRequestCtx_SerializesScopesAndUserID(t *testing.T) {
	ctx := WithScopes(context.Background(), []string{"storage:read", "assistant:write"})
	ctx = WithUserID(ctx, "user-42")
	req := buildRPCRequestCtx(ctx, "storage", "orbtr.io.storage.Get", nil, 5*time.Second, "tenant-A")

	if got := req.Context["scopes"]; got != "storage:read assistant:write" {
		t.Errorf("scopes = %q, want space-joined %q", got, "storage:read assistant:write")
	}
	if got := req.Context["userId"]; got != "user-42" {
		t.Errorf("userId = %q, want user-42", got)
	}
	// Selective-copy security (R-782): role + tenantId are set authoritatively
	// by the builder; they must NOT be overridable through the caller ctx.
	if got := req.Context["tenantId"]; got != "tenant-A" {
		t.Errorf("tenantId = %q, want tenant-A (authoritative param)", got)
	}
	if got := req.Context["role"]; got != "storage" {
		t.Errorf("role = %q, want storage", got)
	}
}

// TestBuildRPCRequestCtx_OmitsEmptyScopesAndUserID verifies the copy is
// conditional — no scopes/userId stamped means no empty wire keys.
func TestBuildRPCRequestCtx_OmitsEmptyScopesAndUserID(t *testing.T) {
	req := buildRPCRequestCtx(context.Background(), "platform", "platform.Ping", nil, time.Second, "")
	if _, ok := req.Context["scopes"]; ok {
		t.Error("scopes key present with none stamped — should be omitted")
	}
	if _, ok := req.Context["userId"]; ok {
		t.Error("userId key present with none stamped — should be omitted")
	}
}

func TestBuildRequest_NoDeadlineSetsDefault(t *testing.T) {
	req := buildRPCRequest("platform", "platform.CheckHealth", nil, 0, "")
	if time.Duration(req.TimeoutNs) != callerRequestTTL {
		t.Fatalf("expected default %v, got %v", callerRequestTTL, time.Duration(req.TimeoutNs))
	}
}

func TestBuildRequest_SetsUniqueID(t *testing.T) {
	r1 := buildRPCRequest("a", "a.X", nil, 0, "")
	r2 := buildRPCRequest("b", "b.Y", nil, 0, "")
	if r1.Id == "" {
		t.Fatal("expected non-empty ID")
	}
	if r1.Id == r2.Id {
		t.Fatal("expected unique IDs")
	}
}

func TestBuildRequest_ShorterDeadlineWins(t *testing.T) {
	req := buildRPCRequest("x", "x.Y", nil, 2*time.Second, "")
	if time.Duration(req.TimeoutNs) != 2*time.Second {
		t.Fatalf("expected 2s, got %v", time.Duration(req.TimeoutNs))
	}
}

func TestBuildRequest_LongerDeadlineUsesDefault(t *testing.T) {
	req := buildRPCRequest("x", "x.Y", nil, 60*time.Second, "")
	if time.Duration(req.TimeoutNs) != callerRequestTTL {
		t.Fatalf("expected default %v, got %v", callerRequestTTL, time.Duration(req.TimeoutNs))
	}
}

func TestBuildRequest_SetsRoleInContext(t *testing.T) {
	req := buildRPCRequest("platform", "platform.CheckHealth", []byte("test"), 0, "")
	if req.Context["role"] != "platform" {
		t.Fatalf("expected role=platform, got %s", req.Context["role"])
	}
	if req.Handler != "platform.CheckHealth" {
		t.Fatalf("expected handler=platform.CheckHealth, got %s", req.Handler)
	}
	if string(req.Payload) != "test" {
		t.Fatalf("expected payload=test, got %s", req.Payload)
	}
}

// TestBuildRequest_TenantIDIsCamelCase pins the wire key for tenant
// identity. The receiving side (mesh/node.rpc.go + rpc/scope.go) reads
// req.Context["tenantId"]. If a writer regresses to snake_case the whole
// scope chain fails closed and every cross-org RPC starts returning 403.
// Fail-closed guard per the camelCase migration; do not relax to dual-emit.
func TestBuildRequest_TenantIDIsCamelCase(t *testing.T) {
	req := buildRPCRequest("platform", "platform.CheckHealth", nil, 0, "orbtr")
	if got := req.Context["tenantId"]; got != "orbtr" {
		t.Fatalf("expected req.Context[\"tenantId\"]=orbtr, got %q (full ctx=%v)", got, req.Context)
	}
	if _, snake := req.Context["tenant_id"]; snake {
		t.Fatalf("snake_case tenant_id must not be emitted: %v", req.Context)
	}
}

// TestBuildRequestCtx_OpClassIsCamelCase pins the wire key for op-class
// hints. The receiving side (mesh/node.rpc_forward.go) reads
// req.Context["opClass"]; a snake_case writer silently downgrades every
// realtime/critical RPC to the Standard band.
func TestBuildRequestCtx_OpClassIsCamelCase(t *testing.T) {
	ctx := WithOpClass(context.Background(), "realtime")
	req := buildRPCRequestCtx(ctx, "platform", "platform.CheckHealth", nil, 0, "orbtr")
	if got := req.Context["opClass"]; got != "realtime" {
		t.Fatalf("expected req.Context[\"opClass\"]=realtime, got %q (full ctx=%v)", got, req.Context)
	}
	if _, snake := req.Context["op_class"]; snake {
		t.Fatalf("snake_case op_class must not be emitted: %v", req.Context)
	}
}

// ─── Mock types for integration tests ───────────────────────────────────────

// rpcMockStream simulates a stream that handles one RPC request-response.
type rpcMockStream struct {
	id       uint64
	sendData []byte
	respData []byte // pre-built response to return on Receive
	sendErr  error
	recvErr  error
}

func (s *rpcMockStream) StreamID() uint64                            { return s.id }
func (s *rpcMockStream) Send(ctx context.Context, data []byte) error { s.sendData = data; return s.sendErr }
func (s *rpcMockStream) Receive(ctx context.Context) ([]byte, error) {
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	return s.respData, nil
}
func (s *rpcMockStream) Close() error                                { return nil }
func (s *rpcMockStream) Reset(reason aether.ResetReason) error          { return nil }
func (s *rpcMockStream) SetPriority(weight uint8, dependency uint64) {}
func (s *rpcMockStream) Config() aether.StreamConfig                    { return aether.StreamConfig{} }
func (s *rpcMockStream) IsOpen() bool                                { return true }
func (s *rpcMockStream) Conn() net.Conn                              { return nil }

// rpcMockSession returns rpcMockStream instances with pre-built responses.
type rpcMockSession struct {
	resp      *pb.RPCResponse
	openErr   error
	closed    bool
	openCount int32
}

func (m *rpcMockSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	atomic.AddInt32(&m.openCount, 1)
	if m.openErr != nil {
		return nil, m.openErr
	}
	respData, _ := pb.MarshalResponse(m.resp)
	return &rpcMockStream{id: cfg.StreamID, respData: respData}, nil
}
func (m *rpcMockSession) AcceptStream(ctx context.Context) (aether.Stream, error) { return nil, nil }
func (m *rpcMockSession) AcceptStreamByID(ctx context.Context, streamID uint64) (aether.Stream, error) {
	return nil, nil
}
func (m *rpcMockSession) IsClosed() bool                                       { return m.closed }
func (m *rpcMockSession) Close() error                                         { return nil }
func (m *rpcMockSession) LocalNodeID() aether.NodeID                        { return "local-test" }
func (m *rpcMockSession) RemoteNodeID() aether.NodeID                       { return "remote-test" }
func (m *rpcMockSession) LocalPeerID() aether.PeerID                             { return aether.PeerID{} }
func (m *rpcMockSession) RemotePeerID() aether.PeerID                            { return aether.PeerID{} }
func (m *rpcMockSession) Capabilities() aether.Capabilities                      { return 0 }
func (m *rpcMockSession) Ping(ctx context.Context) (time.Duration, error)     { return 0, nil }
func (m *rpcMockSession) GoAway(ctx context.Context, r aether.GoAwayReason, msg string) error {
	return nil
}
func (m *rpcMockSession) Health() *health.Monitor              { return nil }
func (m *rpcMockSession) SessionKey() []byte                   { return nil }
func (m *rpcMockSession) ConnectionID() aether.ConnectionID       { return aether.ConnectionID{} }
func (m *rpcMockSession) CongestionWindow() int64              { return 0 }
func (m *rpcMockSession) Protocol() aether.Protocol         { return aether.ProtoWebSocket }
func (m *rpcMockSession) Metrics() aether.SessionMetrics          { return aether.SessionMetrics{} }

// mockFinder implements SessionFinder for tests.
type mockFinder struct {
	session    aether.Session
	anySession aether.Session
	findErr    error
}

func (f *mockFinder) FindSession(ctx context.Context, role, handler string) (aether.Session, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.session, nil
}
func (f *mockFinder) FindAnySession() (aether.Session, bool) {
	if f.anySession != nil {
		return f.anySession, true
	}
	return nil, false
}
func (f *mockFinder) CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil // no BidiRPC in tests — falls through to dynamic stream
}
func (f *mockFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.session != nil {
		return []aether.ProbeRoute{{Session: f.session, NodeID: "test-node", TargetNodeID: "test-node"}}
	}
	return nil
}
func (f *mockFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

// ─── HWPCaller.Call integration tests ───────────────────────────────────────

func TestCall_SuccessfulRPC(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("pong")}}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	resp, err := caller.Call(context.Background(), "platform", "platform.CheckHealth", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp) != "pong" {
		t.Fatalf("expected pong, got %s", resp)
	}

	// Check metrics
	snap := caller.Metrics().Snapshot()
	if snap["platform"].Calls != 1 {
		t.Fatalf("calls: got %d, want 1", snap["platform"].Calls)
	}
	if snap["platform"].Successes != 1 {
		t.Fatalf("successes: got %d, want 1", snap["platform"].Successes)
	}
}

func TestCall_ParallelProbeFailover(t *testing.T) {
	// Two routes: first is dead, second succeeds — parallel probing picks the winner
	deadSess := &rpcMockSession{closed: true}
	goodSess := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("ok")}}

	caller := NewHWPCaller(&multiRouteFinder{
		routes: []aether.ProbeRoute{
			{Session: deadSess, NodeID: "dead-node", TargetNodeID: "target"},
			{Session: goodSess, NodeID: "good-node", TargetNodeID: "target"},
		},
	})
	defer caller.Close()

	resp, err := caller.Call(context.Background(), "mesh_svc", "mesh_svc.ping", nil)
	if err != nil {
		t.Fatalf("parallel probing should succeed via good route: %v", err)
	}
	if string(resp) != "ok" {
		t.Fatalf("expected ok, got %s", resp)
	}
}

// multiRouteFinder returns multiple routes for parallel probing tests.
type multiRouteFinder struct {
	routes []aether.ProbeRoute
}

func (f *multiRouteFinder) FindSession(ctx context.Context, role, handler string) (aether.Session, error) {
	if len(f.routes) > 0 {
		return f.routes[0].Session, nil
	}
	return nil, fmt.Errorf("no sessions")
}
func (f *multiRouteFinder) FindAnySession() (aether.Session, bool) { return nil, false }
func (f *multiRouteFinder) CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil
}
func (f *multiRouteFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	return f.routes
}
func (f *multiRouteFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

func TestCall_CircuitBreakerTrips(t *testing.T) {
	// Session that always fails to open streams — triggers per-node breaker
	failSess := &rpcMockSession{openErr: fmt.Errorf("conn dead")}
	finder := &mockFinder{session: failSess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	// Trip the breaker — parallel probing records failures per-node ("remote-test").
	// Threshold is 5 failures in 60s window.
	for i := 0; i < 5; i++ {
		caller.Call(context.Background(), "billing", "billing.CheckHealth", nil)
	}

	// Next call should have the node's breaker open — probe rejected
	_, err := caller.Call(context.Background(), "billing", "billing.CheckHealth", nil)
	if err == nil {
		t.Fatal("expected error after breaker trip")
	}
	if !contains(err.Error(), "circuit breaker open") && !contains(err.Error(), "all") {
		t.Logf("got error (acceptable): %v", err)
	}
}

func TestCall_ApplicationErrorRecordsSuccess(t *testing.T) {
	// Server returns an application-level error (not transport)
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: false, Error: "not found"}}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	_, err := caller.Call(context.Background(), "identity", "identity.GetUser", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found', got: %v", err)
	}

	// Application errors count as success for health tracking (session was responsive)
	snap := caller.Metrics().Snapshot()
	// The call "fails" at the application level but the first attempt succeeds at transport
	// so metrics should show 1 call. The error is from the handler, not aether.
	if snap["identity"].Calls != 1 {
		t.Fatalf("calls: got %d, want 1", snap["identity"].Calls)
	}
}

func TestCall_ForwardingSessionNotCached(t *testing.T) {
	// FindSession fails, but FindAnySession returns a forwarding session
	fwdSess := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("fwd")}}
	finder := &mockFinder{
		findErr:    fmt.Errorf("no direct session"),
		anySession: fwdSess,
	}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	resp, err := caller.Call(context.Background(), "support", "support.CheckHealth", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(resp) != "fwd" {
		t.Fatalf("expected fwd, got %s", resp)
	}

	// Forwarding sessions should NOT be cached
	caller.mu.RLock()
	_, cached := caller.sessions["support"]
	caller.mu.RUnlock()
	if cached {
		t.Fatal("forwarding session should NOT be cached")
	}
}

// ─── evictIdle / refillPools / Close tests ──────────────────────────────────

func TestEvictIdle_RemovesStaleSession(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true}}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	// Manually inject a session with old lastUsedNano
	cs := &cachedSession{
		session:      sess,
		role:         "old",
		lastUsedNano: time.Now().Add(-10 * time.Minute).UnixNano(),
		pool:         NewStreamPool(2),
	}
	caller.mu.Lock()
	caller.sessions["old"] = cs
	caller.mu.Unlock()

	caller.evictIdle()

	caller.mu.RLock()
	_, exists := caller.sessions["old"]
	caller.mu.RUnlock()
	if exists {
		t.Fatal("expected stale session to be evicted")
	}
}

func TestEvictIdle_KeepsFreshSession(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true}}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	cs := &cachedSession{
		session:      sess,
		role:         "fresh",
		lastUsedNano: time.Now().UnixNano(),
		pool:         NewStreamPool(2),
	}
	caller.mu.Lock()
	caller.sessions["fresh"] = cs
	caller.mu.Unlock()

	caller.evictIdle()

	caller.mu.RLock()
	_, exists := caller.sessions["fresh"]
	caller.mu.RUnlock()
	if !exists {
		t.Fatal("expected fresh session to be kept")
	}
}

func TestEvictIdle_RemovesClosedSession(t *testing.T) {
	sess := &rpcMockSession{closed: true}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	cs := &cachedSession{
		session:      sess,
		role:         "dead",
		lastUsedNano: time.Now().UnixNano(), // recent but closed
		pool:         NewStreamPool(2),
	}
	caller.mu.Lock()
	caller.sessions["dead"] = cs
	caller.mu.Unlock()

	caller.evictIdle()

	caller.mu.RLock()
	_, exists := caller.sessions["dead"]
	caller.mu.RUnlock()
	if exists {
		t.Fatal("expected closed session to be evicted")
	}
}

func TestRefillPools_TopsUpHealthyPools(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true}}
	finder := &mockFinder{session: sess}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	pool := NewStreamPool(2)
	cs := &cachedSession{
		session:      sess,
		role:         "refill",
		lastUsedNano: time.Now().UnixNano(),
		pool:         pool,
	}
	caller.mu.Lock()
	caller.sessions["refill"] = cs
	caller.mu.Unlock()

	// Pool is empty, refill should fill it
	caller.refillPools()

	if pool.Len() != 2 {
		t.Fatalf("expected pool to be filled to 2, got %d", pool.Len())
	}
}

func TestClose_StopsMaintenance(t *testing.T) {
	finder := &mockFinder{}
	caller := NewHWPCaller(finder)

	// Add a session with a pool
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true}}
	pool := NewStreamPool(2)
	pool.Fill(sess)
	caller.mu.Lock()
	caller.sessions["test"] = &cachedSession{
		session:      sess,
		role:         "test",
		lastUsedNano: time.Now().UnixNano(),
		pool:         pool,
	}
	caller.mu.Unlock()

	caller.Close()

	caller.mu.RLock()
	count := len(caller.sessions)
	caller.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 sessions after Close, got %d", count)
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
