/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"testing"
)

func TestEnforceScope_None(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeNone}
	if err := EnforceScope(context.Background(), h, nil); err != nil {
		t.Fatalf("ScopeNone should always pass: %v", err)
	}
}

// TestParseScopes verifies #K-32 wire decoding: space-joined → slice, with
// empty + stray-space robustness (strings.Fields drops empty tokens, so no
// phantom scope from a double space).
func TestParseScopes(t *testing.T) {
	cases := []struct {
		raw  string
		want []string
	}{
		{"", nil},
		{"storage:read", []string{"storage:read"}},
		{"storage:read assistant:write", []string{"storage:read", "assistant:write"}},
		{"  a   b  ", []string{"a", "b"}},
	}
	for _, c := range cases {
		got := ParseScopes(c.raw)
		if len(got) != len(c.want) {
			t.Errorf("ParseScopes(%q) = %v, want %v", c.raw, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("ParseScopes(%q)[%d] = %q, want %q", c.raw, i, got[i], c.want[i])
			}
		}
	}
}

// TestScopesFromContext_RoundTrip verifies the receive-side accessor decodes
// scopes surfaced onto ctx via WithRPCContext.
func TestScopesFromContext_RoundTrip(t *testing.T) {
	ctx := WithRPCContext(context.Background(), map[string]string{"scopes": "a b c"})
	got := ScopesFromContext(ctx)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("ScopesFromContext = %v, want [a b c]", got)
	}
	if s := ScopesFromContext(context.Background()); s != nil {
		t.Errorf("ScopesFromContext on bare ctx = %v, want nil", s)
	}
}

// TestBridgeWireIdentity_NilSafe verifies the send-side bridge is a no-op
// when the rpc context map is absent (no panic, ctx unchanged), and does not
// panic on a populated map.
func TestBridgeWireIdentity_NilSafe(t *testing.T) {
	ctx := context.Background()
	if got := bridgeWireIdentity(ctx); got != ctx {
		t.Error("bridgeWireIdentity on bare ctx should return ctx unchanged")
	}
	ctx = WithRPCContext(context.Background(), map[string]string{"scopes": "x y", "userId": "u1"})
	_ = bridgeWireIdentity(ctx) // must not panic
}

func TestEnforceScope_Platform(t *testing.T) {
	h := &Handler{Namespace: "hstles", Domain: "identity", Operation: "Mint", Scope: ScopePlatform}
	platforms := NewPlatformTenants("orbtr", "hstles")

	// Caller without tenant — denied
	if err := EnforceScope(context.Background(), h, platforms); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing tenant, got %v", err)
	}

	// Caller with non-platform tenant — denied
	ctx := WithRPCContext(context.Background(), map[string]string{"tenantId": "stranger"})
	if err := EnforceScope(ctx, h, platforms); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for stranger tenant, got %v", err)
	}

	// Caller with platform tenant — pass
	ctx = WithRPCContext(context.Background(), map[string]string{"tenantId": "orbtr"})
	if err := EnforceScope(ctx, h, platforms); err != nil {
		t.Errorf("expected pass for platform tenant, got %v", err)
	}
}

func TestEnforceScope_Tenant(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeTenant}
	if err := EnforceScope(context.Background(), h, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing tenant, got %v", err)
	}
	ctx := WithRPCContext(context.Background(), map[string]string{"tenantId": "orbtr"})
	if err := EnforceScope(ctx, h, nil); err != nil {
		t.Errorf("expected pass with tenant, got %v", err)
	}
}

func TestEnforceScope_Org(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeOrg}

	// Tenant only — denied for missing org
	ctx := WithRPCContext(context.Background(), map[string]string{"tenantId": "orbtr"})
	if err := EnforceScope(ctx, h, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing org, got %v", err)
	}

	// Tenant + org — pass
	ctx = WithRPCContext(context.Background(), map[string]string{
		"tenantId": "orbtr",
		"orgId":    "acme",
	})
	if err := EnforceScope(ctx, h, nil); err != nil {
		t.Errorf("expected pass with tenant+org, got %v", err)
	}
}

func TestEnforceScope_User(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeUser}

	ctx := WithRPCContext(context.Background(), map[string]string{"tenantId": "orbtr"})
	if err := EnforceScope(ctx, h, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing user, got %v", err)
	}

	ctx = WithRPCContext(context.Background(), map[string]string{
		"tenantId": "orbtr",
		"userId":   "u1",
	})
	if err := EnforceScope(ctx, h, nil); err != nil {
		t.Errorf("expected pass with tenant+user, got %v", err)
	}
}

func TestEnforceScope_UnknownScope(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: "bogus"}
	if err := EnforceScope(context.Background(), h, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for unknown scope, got %v", err)
	}
}

func TestEnforceScope_NilHandler(t *testing.T) {
	if err := EnforceScope(context.Background(), nil, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for nil handler, got %v", err)
	}
}

func TestWithRPCContext_Merge(t *testing.T) {
	ctx := WithRPCContext(context.Background(), map[string]string{"a": "1", "b": "2"})
	ctx = WithRPCContext(ctx, map[string]string{"b": "B", "c": "3"})

	m := rpcContextMap(ctx)
	if m["a"] != "1" || m["b"] != "B" || m["c"] != "3" {
		t.Errorf("merge result wrong: %+v", m)
	}
}

func TestPlatformTenants_NilSafe(t *testing.T) {
	var p PlatformTenants
	if p.IsPlatform("anything") {
		t.Errorf("nil PlatformTenants must answer false")
	}
}

func TestExtractRole(t *testing.T) {
	cases := []struct{ in, out string }{
		{"hstles.identity.MintSession", "hstles.identity"},
		{"orbtr.device.List", "orbtr.device"},
		// malformed inputs all return "" — previous impl returned the whole
		// string for these.
		{"bogus", ""},
		{"bogus.x", ""},      // only one dot
		{".x.y", ""},         // empty namespace
		{"x.y.", ""},         // empty operation
		{"x..y", ""},         // empty domain
		{"", ""},             // empty
		{"....", ""},         // dots only
	}
	for _, c := range cases {
		if got := ExtractRole(c.in); got != c.out {
			t.Errorf("ExtractRole(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestRegistry_RegisterCopiesSlices(t *testing.T) {
	reg := NewRegistry()
	tags := []string{"x"}
	if err := reg.Register(Handler{Namespace: "n", Domain: "d", Operation: "P", Tags: tags}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Mutate caller's slice — registry must not see it
	tags[0] = "y"
	h, ok := reg.Get("n.d.P")
	if !ok {
		t.Fatal("missing")
	}
	if h.Tags[0] != "x" {
		t.Errorf("registry Tags aliased caller's slice: got %q", h.Tags[0])
	}
}
