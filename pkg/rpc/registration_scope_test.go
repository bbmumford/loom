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
	var unset ScopeTag
	if unset == ScopeNone {
		t.Fatalf("zero ScopeTag and ScopeNone are the same value %q; omission must differ from deliberate public opt-out", unset)
	}
	if ScopeNone == "" {
		t.Fatal("ScopeNone is the empty string; the zero value is still permissive")
	}
	// The caller-facing type is ScopeTag; its canonical underlying defined
	// type is scope.Scope. What matters is that it is a DEFINED type (not a
	// bare string), so an arbitrary string is not assignable as a scope.
	typ := reflect.TypeOf(ScopeNone)
	if typ.Name() != "ScopeTag" {
		t.Fatalf("scope tag is not a defined type: reflect name = %q", typ.Name())
	}
	if typ.Kind() != reflect.String {
		t.Fatalf("scope tag underlying kind = %v, want string", typ.Kind())
	}
}

// TestRegistryRejectsSentinelScopeBeforeMutation proves loom refuses ONLY the
// two non-declarable sentinels at registration — the ScopeUnset zero value and
// the ScopeUnknown proto-decay marker — and does so before mutating any index.
// A consumer-defined opaque axis is NOT a sentinel and is admitted (see
// TestRegistryAcceptsOpaqueConsumerAxis); loom no longer enumerates a fixed
// ladder here.
func TestRegistryRejectsSentinelScopeBeforeMutation(t *testing.T) {
	cases := []struct {
		name  string
		scope ScopeTag
	}{
		{name: "zero"},
		{name: "unknown", scope: ScopeUnknown},
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
				t.Fatalf("Register accepted sentinel scope %q", tc.scope)
			}
			if !strings.Contains(err.Error(), "non-declarable scope") {
				t.Fatalf("Register error = %q, want non-declarable-scope diagnostic", err)
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

// TestRegistryAcceptsOpaqueConsumerAxis proves loom's taxonomy-agnostic
// admission: a consumer-defined opaque scope tag loom does not bless registers
// successfully (its meaning is the consumer PresencePolicy's to judge, and an
// unrecognised tag fails CLOSED at EnforceScope — see
// TestEnforceScope_OpaqueTagFailsClosed).
func TestRegistryAcceptsOpaqueConsumerAxis(t *testing.T) {
	reg := NewRegistry()
	h := Handler{
		Namespace: "test",
		Domain:    "scope",
		Operation: "OpaqueAxis",
		Scope:     ScopeTag("partition"),
	}
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register opaque consumer axis: %v", err)
	}
	if got := reg.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}
	stored, ok := reg.Get(h.FQN())
	if !ok || stored.Scope != ScopeTag("partition") {
		t.Fatalf("stored opaque axis = (%+v, %v)", stored, ok)
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
	if stored.Scope() != handlers.ScopeNone {
		t.Fatalf("exported scope = %q, want explicit ScopeNone", stored.Scope())
	}
}
