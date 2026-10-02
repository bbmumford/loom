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

// meshHandler is a minimal ports.SecureHandler for the scope-check test.
type meshHandler struct {
	name   string
	scopes []string
}

func (h meshHandler) Name() string                { return h.name }
func (h meshHandler) RequiresAuth() bool          { return true }
func (h meshHandler) AllowedAuthTypes() []string  { return nil }
func (h meshHandler) Scopes() []string            { return h.scopes }
func (h meshHandler) Scope() ports.Scope          { return "" }
func (h meshHandler) AllowedPartitions() []string { return nil }

// TestValidateExecutionAuthChecksTheHandlerScopes: the default validator
// denies a handler's required scope when no identity is stamped, admits it
// when the stamped identity holds it, and denies a scope the identity lacks.
func TestValidateExecutionAuthChecksTheHandlerScopes(t *testing.T) {
	v := Default()

	// No identity stamped → denied (fail closed).
	if err := v.ValidateExecutionAuth(context.Background(),
		meshHandler{name: "storage.Get", scopes: []string{"storage:read"}}); err == nil {
		t.Fatal("expected deny with no identity")
	}

	// Identity holding the required scope → allowed.
	ctx := WithAuth(context.Background(), "user-1", "", []string{"storage:read", "storage:write"})
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
		t.Error("WithAuth should mark the ctx authenticated")
	}
}

func TestDefaultExecutionPartitionIDReadsWithoutMinting(t *testing.T) {
	reader, ok := Default().(ports.PartitionPrincipalReader)
	if !ok {
		t.Fatal("default validator must implement ports.PartitionPrincipalReader")
	}

	if partitionID, ok := reader.ExecutionPartitionID(context.Background()); ok || partitionID != "" {
		t.Fatalf("empty context principal = (%q, %v), want (\"\", false)", partitionID, ok)
	}

	ctx := WithPartitionID(context.Background(), "org-authoritative")
	if partitionID, ok := reader.ExecutionPartitionID(ctx); !ok || partitionID != "org-authoritative" {
		t.Fatalf(
			"established context principal = (%q, %v), want (org-authoritative, true)",
			partitionID,
			ok,
		)
	}
}
