/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"context"
	"net/http"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// ownedFixture registers one handler with every caller-writable field set and
// returns the registry, the handler's FQN and the caller's own inputs.
func ownedFixture(t *testing.T) (*Registry, string, *Handler) {
	t.Helper()
	in := &Handler{
		Namespace: "test", Domain: "owned", Operation: "Op",
		Func: func(context.Context, proto.Message) (proto.Message, error) {
			return &wrapperspb.StringValue{Value: "ran"}, nil
		},
		Request:     (*wrapperspb.StringValue)(nil),
		Response:    (*wrapperspb.StringValue)(nil),
		Middleware:  []Middleware{func(next http.Handler) http.Handler { return next }},
		Scope:       ScopeNone,
		Requirement: &scope.Requirement{Kind: scope.ArmCapability, Cap: "test:owned.read"},
		Tags:        []string{"read"},
		WebhookSignature: &WebhookSignatureConfig{
			Algorithm: "stripe", SecretEnvVar: "TEST_OWNED_SECRET", Header: "Stripe-Signature",
		},
	}
	reg := NewRegistry()
	if err := reg.Register(*in); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return reg, in.FQN(), in
}

// stored reports what the registry serves for fqn, read through a fresh Get.
func stored(t *testing.T, reg *Registry, fqn string) *Handler {
	t.Helper()
	h, ok := reg.Get(fqn)
	if !ok {
		t.Fatalf("Get(%s): not registered", fqn)
	}
	return h
}

// unchanged fails t when the registry no longer serves the fixture's values.
func unchanged(t *testing.T, what string, h *Handler) {
	t.Helper()
	switch {
	case h.Func == nil:
		t.Errorf("%s cleared the stored Func", what)
	case h.Scope != ScopeNone:
		t.Errorf("%s changed the stored Scope to %q", what, h.Scope)
	case h.Requirement == nil || h.Requirement.Cap != "test:owned.read":
		t.Errorf("%s changed the stored Requirement to %+v", what, h.Requirement)
	case len(h.Tags) != 1 || h.Tags[0] != "read":
		t.Errorf("%s changed the stored Tags to %v", what, h.Tags)
	case len(h.Middleware) != 1:
		t.Errorf("%s changed the stored Middleware to %d entries", what, len(h.Middleware))
	case h.WebhookSignature == nil || h.WebhookSignature.Algorithm != "stripe":
		t.Errorf("%s changed the stored WebhookSignature to %+v", what, h.WebhookSignature)
	}
}

// scribble writes every caller-writable field of h.
func scribble(h *Handler) {
	h.Func = nil
	h.Scope = ScopeAllowlisted
	if h.Requirement != nil {
		h.Requirement.Cap = "test:owned.write"
	}
	if len(h.Tags) > 0 {
		h.Tags[0] = "write"
	}
	if len(h.Middleware) > 0 {
		h.Middleware[0] = nil
	}
	if h.WebhookSignature != nil {
		h.WebhookSignature.Algorithm = "none"
	}
}

// The registry serves what was registered and nothing else: a write to the
// caller's own Requirement or WebhookSignature after Register, or to any handler
// Get, All, ByRole, ByNamespace or MetadataFor hands out, changes neither what a
// fresh Get reads nor what the mesh export dispatches.
func TestTheRegistryHandsOutCopiesItsCallersCannotWriteThrough(t *testing.T) {
	reg, fqn, in := ownedFixture(t)
	in.Requirement.Cap = "test:owned.write"
	in.WebhookSignature.Algorithm = "none"
	unchanged(t, "a write to the registered input", stored(t, reg, fqn))

	scribble(stored(t, reg, fqn))
	unchanged(t, "a write to what Get returned", stored(t, reg, fqn))
	for _, h := range reg.All() {
		scribble(h)
	}
	unchanged(t, "a write to what All returned", stored(t, reg, fqn))
	for _, h := range reg.ByRole("test.owned") {
		scribble(h)
	}
	unchanged(t, "a write to what ByRole returned", stored(t, reg, fqn))
	for _, h := range reg.ByNamespace("test") {
		scribble(h)
	}
	unchanged(t, "a write to what ByNamespace returned", stored(t, reg, fqn))
	meta, _ := reg.MetadataFor(fqn)
	scribble(&meta)
	unchanged(t, "a write to what MetadataFor returned", stored(t, reg, fqn))
}
