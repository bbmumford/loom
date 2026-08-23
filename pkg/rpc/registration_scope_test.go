/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
)

func TestTenantScopeHasInvalidZeroAndExplicitNone(t *testing.T) {
	var unset TenantScope
	if unset == ScopeNone {
		t.Fatalf("zero TenantScope and ScopeNone are the same value %q; omission must differ from deliberate public opt-out", unset)
	}
	if ScopeNone == "" {
		t.Fatal("ScopeNone is the empty string; the zero value is still permissive")
	}
	typ := reflect.TypeOf(ScopeNone)
	if typ.Name() != "TenantScope" {
		t.Fatalf("TenantScope is not a defined type: reflect name = %q", typ.Name())
	}
}

func TestRegistryRejectsUndeclaredTenantScopeBeforeMutation(t *testing.T) {
	cases := []struct {
		name  string
		scope TenantScope
	}{
		{name: "zero"},
		{name: "unknown", scope: ScopeUnknown},
		{name: "garbage", scope: TenantScope("tennant")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			err := reg.Register(Handler{
				Namespace: "test",
				Domain:    "scope",
				Operation: "Rejected",
				Scope:     tc.scope,
			})
			if err == nil {
				t.Fatalf("Register accepted undeclared scope %q", tc.scope)
			}
			if !strings.Contains(err.Error(), "tenant scope") {
				t.Fatalf("Register error = %q, want tenant-scope diagnostic", err)
			}
			if got := reg.Count(); got != 0 {
				t.Fatalf("rejected registration mutated handler count: got %d", got)
			}
			if got := reg.Roles(); len(got) != 0 {
				t.Fatalf("rejected registration mutated role index: %v", got)
			}
		})
	}
}

func TestRegistryAcceptsExplicitTenantScopeNone(t *testing.T) {
	reg := NewRegistry()
	h := Handler{
		Namespace: "test",
		Domain:    "scope",
		Operation: "Public",
		Scope:     ScopeNone,
	}
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register explicit ScopeNone: %v", err)
	}
	if got := reg.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}
	stored, ok := reg.Get(h.FQN())
	if !ok || stored.Scope != ScopeNone {
		t.Fatalf("stored explicit ScopeNone = (%+v, %v)", stored, ok)
	}
}

func TestRegistryExportPreservesExplicitTenantScopeNone(t *testing.T) {
	source := NewRegistry()
	h := Handler{
		Namespace: "test",
		Domain:    "scope",
		Operation: "ExportedPublic",
		Scope:     ScopeNone,
	}
	if err := source.Register(h); err != nil {
		t.Fatalf("Register explicit ScopeNone: %v", err)
	}

	target := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(target); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	stored, ok := target.GetMeta(h.FQN())
	if !ok {
		t.Fatalf("exported handler %q missing from target registry", h.FQN())
	}
	if stored.TenantScope() != handlers.TenantScopeNone {
		t.Fatalf("exported scope = %q, want explicit TenantScopeNone", stored.TenantScope())
	}
}
