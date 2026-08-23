/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package securityctx

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/ports"
)

// meshHandler is a minimal ports.SecureHandler for the WithWireIdentity test.
type meshHandler struct {
	name   string
	scopes []string
}

func (h meshHandler) Name() string                   { return h.name }
func (h meshHandler) RequiresAuth() bool             { return true }
func (h meshHandler) AllowedAuthTypes() []string     { return nil }
func (h meshHandler) Scopes() []string               { return h.scopes }
func (h meshHandler) TenantScope() ports.TenantScope { return "" }
func (h meshHandler) AllowedTenants() []string       { return nil }

// TestWithWireIdentity_EnablesScopeEnforcement verifies #K-32: the default
// validator implements ports.ScopeStamper, and a mesh-propagated principal
// carrying the required scope passes ValidateExecutionAuth while a missing
// scope is denied (fail closed) — the receive-side half of the scope plumb.
func TestWithWireIdentity_EnablesScopeEnforcement(t *testing.T) {
	v := Default()
	ss, ok := v.(ports.ScopeStamper)
	if !ok {
		t.Fatal("default validator must implement ports.ScopeStamper")
	}

	// No principal stamped → denied (fail closed).
	if err := v.ValidateExecutionAuth(context.Background(),
		meshHandler{name: "storage.Get", scopes: []string{"storage:read"}}); err == nil {
		t.Fatal("expected deny with no mesh principal")
	}

	// Mesh principal carrying the required scope → allowed.
	ctx := ss.WithWireIdentity(context.Background(), "user-1", []string{"storage:read", "storage:write"})
	if err := v.ValidateExecutionAuth(ctx,
		meshHandler{name: "storage.Get", scopes: []string{"storage:read"}}); err != nil {
		t.Errorf("principal with storage:read should pass: %v", err)
	}

	// A required scope the principal lacks → denied.
	if err := v.ValidateExecutionAuth(ctx,
		meshHandler{name: "storage.Admin", scopes: []string{"storage:admin"}}); err == nil {
		t.Error("principal lacking storage:admin should be denied")
	}

	// The raw accessors round-trip the stamped principal.
	if got := ExtractUserID(ctx); got != "user-1" {
		t.Errorf("ExtractUserID = %q, want user-1", got)
	}
	if got := ExtractScopes(ctx); len(got) != 2 {
		t.Errorf("ExtractScopes = %v, want 2 scopes", got)
	}
	if !IsAuthenticated(ctx) {
		t.Error("WithWireIdentity should mark the ctx authenticated")
	}
}

func TestDefaultExecutionTenantIDReadsWithoutMinting(t *testing.T) {
	reader, ok := Default().(ports.TenantPrincipalReader)
	if !ok {
		t.Fatal("default validator must implement ports.TenantPrincipalReader")
	}

	if tenantID, ok := reader.ExecutionTenantID(context.Background()); ok || tenantID != "" {
		t.Fatalf("empty context principal = (%q, %v), want (\"\", false)", tenantID, ok)
	}

	ctx := WithTenantID(context.Background(), "org-authoritative")
	if tenantID, ok := reader.ExecutionTenantID(ctx); !ok || tenantID != "org-authoritative" {
		t.Fatalf(
			"established context principal = (%q, %v), want (org-authoritative, true)",
			tenantID,
			ok,
		)
	}
}
