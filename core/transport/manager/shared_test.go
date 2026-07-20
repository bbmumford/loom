/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
)

func testSharedTransportConfig(t *testing.T) noise.NoiseTransportConfig {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return noise.NoiseTransportConfig{
		PrivateKey:  priv,
		NetworkKeys: []string{"default-shared-key"},
		ListenAddr:  "127.0.0.1:0",
	}
}

func TestSharedTransport_RegisterTenant(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	keys := [][]byte{[]byte("psk-tenant-A")}
	if err := st.RegisterTenant("tenant-A", keys); err != nil {
		t.Fatalf("register: %v", err)
	}
	if st.TenantCount() != 1 {
		t.Errorf("count = %d, want 1", st.TenantCount())
	}
}

func TestSharedTransport_RegisterTenant_Duplicate(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	keys := [][]byte{[]byte("psk-tenant-A")}
	st.RegisterTenant("tenant-A", keys)

	err = st.RegisterTenant("tenant-A", keys)
	if err != ErrSharedTenantExists {
		t.Fatalf("err = %v, want ErrSharedTenantExists", err)
	}
}

func TestSharedTransport_RegisterTenant_NoKeys(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	err = st.RegisterTenant("tenant-A", nil)
	if err != ErrSharedNoKeysForTenant {
		t.Fatalf("err = %v, want ErrSharedNoKeysForTenant", err)
	}
}

func TestSharedTransport_RemoveTenant(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	st.RegisterTenant("tenant-A", [][]byte{[]byte("key")})
	if err := st.RemoveTenant("tenant-A"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if st.TenantCount() != 0 {
		t.Errorf("count = %d, want 0", st.TenantCount())
	}
}

func TestSharedTransport_RemoveTenant_NotFound(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	err = st.RemoveTenant("nonexistent")
	if err != ErrSharedTenantNotFound {
		t.Fatalf("err = %v, want ErrSharedTenantNotFound", err)
	}
}

func TestSharedTransport_ResolveKeys(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	keys := [][]byte{[]byte("active-key"), []byte("old-key")}
	st.RegisterTenant("tenant-A", keys)

	resolved, err := st.ResolveKeys("tenant-A")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(resolved) != 2 {
		t.Errorf("resolved count = %d, want 2", len(resolved))
	}
}

func TestSharedTransport_ResolveKeys_NotFound(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	_, err = st.ResolveKeys("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown tenant")
	}
}

func TestSharedTransport_UpdateTenantKeys(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	st.RegisterTenant("tenant-A", [][]byte{[]byte("old-key")})

	newKeys := [][]byte{[]byte("new-key"), []byte("old-key")}
	if err := st.UpdateTenantKeys("tenant-A", newKeys); err != nil {
		t.Fatalf("update: %v", err)
	}

	resolved, _ := st.ResolveKeys("tenant-A")
	if string(resolved[0]) != "new-key" {
		t.Errorf("active key = %q, want %q", resolved[0], "new-key")
	}
}

func TestSharedTransport_UpdateTenantKeys_NotFound(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	err = st.UpdateTenantKeys("nonexistent", [][]byte{[]byte("key")})
	if err != ErrSharedTenantNotFound {
		t.Fatalf("err = %v, want ErrSharedTenantNotFound", err)
	}
}

func TestSharedTransport_Dial_NoTenantInCtx(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Dial without tenant in context should fail
	_, err = st.Dial(context.Background(), aether.Target{
		NodeID:  "test-node",
		Address: "127.0.0.1:9999",
	})
	if err != ErrSharedNoTenantInCtx {
		t.Fatalf("err = %v, want ErrSharedNoTenantInCtx", err)
	}
}

func TestSharedTransport_Dial_UnknownTenant(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Dial with unknown tenant in context
	ctx := WithTenantID(context.Background(), "unknown-tenant")
	_, err = st.Dial(ctx, aether.Target{
		NodeID:  "test-node",
		Address: "127.0.0.1:9999",
	})
	if err == nil {
		t.Fatal("expected error for unknown tenant")
	}
}

func TestSharedTransport_KeyIsolation(t *testing.T) {
	st, err := NewSharedTransport(testSharedTransportConfig(t))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	keyA := [][]byte{[]byte("psk-alpha")}
	keyB := [][]byte{[]byte("psk-beta")}
	st.RegisterTenant("alpha", keyA)
	st.RegisterTenant("beta", keyB)

	resolvedA, _ := st.ResolveKeys("alpha")
	resolvedB, _ := st.ResolveKeys("beta")

	if string(resolvedA[0]) == string(resolvedB[0]) {
		t.Error("different tenants returned same key")
	}
	if string(resolvedA[0]) != "psk-alpha" {
		t.Errorf("alpha key = %q, want %q", resolvedA[0], "psk-alpha")
	}
	if string(resolvedB[0]) != "psk-beta" {
		t.Errorf("beta key = %q, want %q", resolvedB[0], "psk-beta")
	}
}

func TestTransportManager_ContextInjection(t *testing.T) {
	// Verify that TransportManager.Dial injects tenant ID into context.
	// We use a custom transport that captures the context.
	var capturedCtx context.Context
	captureTx := &contextCapturingTransport{
		captureCtx: func(ctx context.Context) { capturedCtx = ctx },
	}

	mgr := New()
	mgr.Register(aether.ScopeID("test-tenant"), captureTx)

	_, _ = mgr.Dial(context.Background(), "test-tenant", aether.Target{
		NodeID:  "node",
		Address: "127.0.0.1:0",
	})

	if capturedCtx == nil {
		t.Fatal("context was not captured")
	}
	tid, ok := TenantIDFromContext(capturedCtx)
	if !ok {
		t.Fatal("tenant ID not found in context")
	}
	if tid != "test-tenant" {
		t.Errorf("tenant = %q, want %q", tid, "test-tenant")
	}
}

// contextCapturingTransport captures the context passed to Dial for inspection.
type contextCapturingTransport struct {
	captureCtx func(context.Context)
}

func (t *contextCapturingTransport) Dial(ctx context.Context, _ aether.Target) (aether.Connection, error) {
	if t.captureCtx != nil {
		t.captureCtx(ctx)
	}
	return &mockSession{nodeID: "captured"}, nil
}

func (t *contextCapturingTransport) Listen(_ context.Context) (aether.Listener, error) {
	return nil, nil
}

func (t *contextCapturingTransport) Close() error { return nil }
