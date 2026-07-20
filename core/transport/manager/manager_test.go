/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
)

// --- Mock transport for testing ---

type mockSession struct {
	nodeID aether.NodeID
	closed bool
}

func (s *mockSession) Send(_ context.Context, _ []byte) error   { return nil }
func (s *mockSession) Receive(_ context.Context) ([]byte, error) { return nil, nil }
func (s *mockSession) Close() error                              { s.closed = true; return nil }
func (s *mockSession) RemoteAddr() net.Addr                      { return &net.UDPAddr{} }
func (s *mockSession) RemoteNodeID() aether.NodeID               { return s.nodeID }
func (s *mockSession) NetConn() net.Conn                         { return nil }
func (s *mockSession) Protocol() aether.Protocol                 { return aether.ProtoNoise }
func (s *mockSession) OnClose(_ func())                          {}

type mockTransport struct {
	id       string
	dialErr  error
	closed   bool
	closeMu  sync.Mutex
}

func (t *mockTransport) Dial(_ context.Context, target aether.Target) (aether.Connection, error) {
	if t.dialErr != nil {
		return nil, t.dialErr
	}
	return &mockSession{nodeID: target.NodeID}, nil
}

func (t *mockTransport) Listen(_ context.Context) (aether.Listener, error) {
	return nil, errors.New("not implemented")
}

func (t *mockTransport) Close() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.closed = true
	return nil
}

func (t *mockTransport) isClosed() bool {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	return t.closed
}

// --- Tests ---

func TestNew(t *testing.T) {
	mgr := New()
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
	if mgr.Count() != 0 {
		t.Errorf("count = %d, want 0", mgr.Count())
	}
}

func TestRegister_Happy(t *testing.T) {
	mgr := New()
	tr := &mockTransport{id: "orbtr"}

	err := mgr.Register("orbtr", tr)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if mgr.Count() != 1 {
		t.Errorf("count = %d, want 1", mgr.Count())
	}
}

func TestRegister_Duplicate(t *testing.T) {
	mgr := New()
	tr := &mockTransport{id: "orbtr"}

	mgr.Register("orbtr", tr)
	err := mgr.Register("orbtr", tr)
	if !errors.Is(err, ErrTenantAlreadyExists) {
		t.Errorf("err = %v, want ErrTenantAlreadyExists", err)
	}
}

func TestRemove_Happy(t *testing.T) {
	mgr := New()
	tr := &mockTransport{id: "orbtr"}
	mgr.Register("orbtr", tr)

	err := mgr.Remove("orbtr")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if mgr.Count() != 0 {
		t.Errorf("count = %d, want 0", mgr.Count())
	}
}

func TestRemove_NotFound(t *testing.T) {
	mgr := New()
	err := mgr.Remove("nonexistent")
	if !errors.Is(err, ErrTenantNotRegistered) {
		t.Errorf("err = %v, want ErrTenantNotRegistered", err)
	}
}

func TestGet_Happy(t *testing.T) {
	mgr := New()
	tr := &mockTransport{id: "orbtr"}
	mgr.Register("orbtr", tr)

	got, err := mgr.Get("orbtr")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != tr {
		t.Error("got wrong transport instance")
	}
}

func TestGet_NotFound(t *testing.T) {
	mgr := New()
	_, err := mgr.Get("missing")
	if !errors.Is(err, ErrTenantNotRegistered) {
		t.Errorf("err = %v, want ErrTenantNotRegistered", err)
	}
}

func TestDial_RoutesToCorrectTransport(t *testing.T) {
	mgr := New()
	trA := &mockTransport{id: "tenant-a"}
	trB := &mockTransport{id: "tenant-b"}
	mgr.Register("tenant-a", trA)
	mgr.Register("tenant-b", trB)

	target := aether.Target{NodeID: "vl1_test", Address: "127.0.0.1:9000"}

	sess, err := mgr.Dial(context.Background(), "tenant-a", target)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if sess.TenantID() != "tenant-a" {
		t.Errorf("tenant = %q, want %q", sess.TenantID(), "tenant-a")
	}
	if sess.RemoteNodeID() != "vl1_test" {
		t.Errorf("remote = %q, want %q", sess.RemoteNodeID(), "vl1_test")
	}
}

func TestDial_UnknownTenant(t *testing.T) {
	mgr := New()
	target := aether.Target{NodeID: "vl1_test"}

	_, err := mgr.Dial(context.Background(), "unknown", target)
	if !errors.Is(err, ErrTenantNotRegistered) {
		t.Errorf("err = %v, want ErrTenantNotRegistered", err)
	}
}

func TestDial_TransportError(t *testing.T) {
	mgr := New()
	tr := &mockTransport{id: "err-tenant", dialErr: errors.New("connection refused")}
	mgr.Register("err-tenant", tr)

	target := aether.Target{NodeID: "vl1_test"}
	_, err := mgr.Dial(context.Background(), "err-tenant", target)
	if err == nil {
		t.Fatal("expected dial error")
	}
}

func TestListTenants(t *testing.T) {
	mgr := New()
	mgr.Register("alpha", &mockTransport{})
	mgr.Register("beta", &mockTransport{})
	mgr.Register("gamma", &mockTransport{})

	ids := mgr.ListTenants()
	if len(ids) != 3 {
		t.Errorf("len = %d, want 3", len(ids))
	}

	// Verify all expected IDs present
	seen := make(map[aether.ScopeID]bool)
	for _, id := range ids {
		seen[id] = true
	}
	for _, expected := range []aether.ScopeID{"alpha", "beta", "gamma"} {
		if !seen[expected] {
			t.Errorf("missing tenant %q", expected)
		}
	}
}

func TestClose_ClosesAllTransports(t *testing.T) {
	mgr := New()
	trA := &mockTransport{id: "a"}
	trB := &mockTransport{id: "b"}
	mgr.Register("a", trA)
	mgr.Register("b", trB)

	err := mgr.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	if !trA.isClosed() {
		t.Error("transport A not closed")
	}
	if !trB.isClosed() {
		t.Error("transport B not closed")
	}
}

func TestClose_OperationsFailAfter(t *testing.T) {
	mgr := New()
	mgr.Close()

	err := mgr.Register("late", &mockTransport{})
	if !errors.Is(err, ErrManagerClosed) {
		t.Errorf("register after close: %v, want ErrManagerClosed", err)
	}

	_, err = mgr.Get("any")
	if !errors.Is(err, ErrManagerClosed) {
		t.Errorf("get after close: %v, want ErrManagerClosed", err)
	}
}

func TestReplace_Happy(t *testing.T) {
	mgr := New()
	trOld := &mockTransport{id: "old"}
	trNew := &mockTransport{id: "new"}
	mgr.Register("tenant", trOld)

	err := mgr.Replace("tenant", trNew)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}

	got, _ := mgr.Get("tenant")
	if got != trNew {
		t.Error("replace did not swap transport")
	}
}

func TestReplace_NotFound(t *testing.T) {
	mgr := New()
	err := mgr.Replace("missing", &mockTransport{})
	if !errors.Is(err, ErrTenantNotRegistered) {
		t.Errorf("err = %v, want ErrTenantNotRegistered", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	mgr := New()
	var wg sync.WaitGroup

	// Concurrent register/get/list
	for i := range 20 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tid := aether.ScopeID(fmt.Sprintf("t-%d", n))
			mgr.Register(tid, &mockTransport{})
			mgr.Get(tid)
			mgr.ListTenants()
			mgr.Count()
		}(i)
	}
	wg.Wait()

	if mgr.Count() != 20 {
		t.Errorf("count = %d, want 20", mgr.Count())
	}
}

func TestTenantSession_Wrap(t *testing.T) {
	inner := &mockSession{nodeID: "vl1_abc"}
	sess := WrapSession(inner, "my-tenant")

	if sess.TenantID() != "my-tenant" {
		t.Errorf("tenant = %q, want %q", sess.TenantID(), "my-tenant")
	}
	if sess.RemoteNodeID() != "vl1_abc" {
		t.Errorf("remote = %q, want %q", sess.RemoteNodeID(), "vl1_abc")
	}
	if sess.Unwrap() != inner {
		t.Error("unwrap returned wrong session")
	}
}

// --- Key Rotation Tests ---

func makeNoiseTransport(t *testing.T, keys []string) *noise.NoiseTransport {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nt, err := noise.NewNoiseTransport(noise.NoiseTransportConfig{
		PrivateKey:  priv,
		NetworkKeys: keys,
		ListenAddr:  "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	return nt
}

func TestRotateKey_SharedTransport(t *testing.T) {
	st, err := NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:  mustGenKey(t),
		NetworkKeys: []string{"shared-default"},
		ListenAddr:  "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("new shared: %v", err)
	}

	oldKey := []byte("old-psk-alpha")
	st.RegisterTenant("alpha", [][]byte{oldKey})

	mgr := New()
	mgr.Register("alpha", st)

	// Rotate: prepend new key
	newKey := []byte("new-psk-alpha")
	if err := mgr.RotateKey("alpha", newKey); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Verify: ResolveKeys should return [new, old]
	keys, err := st.ResolveKeys("alpha")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys len = %d, want 2", len(keys))
	}
	if !bytes.Equal(keys[0], newKey) {
		t.Errorf("keys[0] = %q, want %q", keys[0], newKey)
	}
	if !bytes.Equal(keys[1], oldKey) {
		t.Errorf("keys[1] = %q, want %q", keys[1], oldKey)
	}
}

func TestRotateKey_DedicatedTransport(t *testing.T) {
	nt := makeNoiseTransport(t, []string{"old-key"})

	mgr := New()
	mgr.Register("beta", nt)

	newKey := []byte("new-key")
	if err := mgr.RotateKey("beta", newKey); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Verify: CurrentKeys should return [new, old]
	keys := nt.CurrentKeys()
	if len(keys) != 2 {
		t.Fatalf("keys len = %d, want 2", len(keys))
	}
	if !bytes.Equal(keys[0], newKey) {
		t.Errorf("keys[0] = %q, want %q", keys[0], newKey)
	}
	if !bytes.Equal(keys[1], []byte("old-key")) {
		t.Errorf("keys[1] = %q, want %q", keys[1], "old-key")
	}
}

func TestRotateKey_UnsupportedTransport(t *testing.T) {
	mgr := New()
	mgr.Register("mock", &mockTransport{})

	err := mgr.RotateKey("mock", []byte("key"))
	if err == nil {
		t.Fatal("expected error for unsupported transport type")
	}
}

func TestRotateKey_UnknownTenant(t *testing.T) {
	mgr := New()

	err := mgr.RotateKey("nonexistent", []byte("key"))
	if !errors.Is(err, ErrTenantNotRegistered) {
		t.Errorf("err = %v, want ErrTenantNotRegistered", err)
	}
}

func TestFinalizeRotation_SharedTransport(t *testing.T) {
	st, err := NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:  mustGenKey(t),
		NetworkKeys: []string{"shared-default"},
		ListenAddr:  "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("new shared: %v", err)
	}

	oldKey := []byte("old-psk")
	st.RegisterTenant("gamma", [][]byte{oldKey})

	mgr := New()
	mgr.Register("gamma", st)

	// Rotate then finalize
	newKey := []byte("new-psk")
	mgr.RotateKey("gamma", newKey)
	if err := mgr.FinalizeRotation("gamma"); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// After finalize: only the new key remains
	keys, _ := st.ResolveKeys("gamma")
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1", len(keys))
	}
	if !bytes.Equal(keys[0], newKey) {
		t.Errorf("keys[0] = %q, want %q", keys[0], newKey)
	}
}

func TestFinalizeRotation_DedicatedTransport(t *testing.T) {
	nt := makeNoiseTransport(t, []string{"old-key"})

	mgr := New()
	mgr.Register("delta", nt)

	mgr.RotateKey("delta", []byte("new-key"))
	if err := mgr.FinalizeRotation("delta"); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	keys := nt.CurrentKeys()
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("new-key")) {
		t.Errorf("keys[0] = %q, want %q", keys[0], "new-key")
	}
}

func TestFinalizeRotation_AlreadyFinalized(t *testing.T) {
	nt := makeNoiseTransport(t, []string{"only-key"})

	mgr := New()
	mgr.Register("epsilon", nt)

	// Finalize with a single key should be a no-op
	if err := mgr.FinalizeRotation("epsilon"); err != nil {
		t.Fatalf("finalize no-op: %v", err)
	}

	keys := nt.CurrentKeys()
	if len(keys) != 1 {
		t.Fatalf("keys len = %d, want 1", len(keys))
	}
}

func mustGenKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}
