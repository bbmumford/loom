/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"strings"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// These probes put a partition key into the mutable RPC map in both the
// camelCase and the snake_case spelling. Spelling is irrelevant: the map is not
// authority in either spelling. Only the typed server-established identity may
// satisfy a scope tier.
const probeTenant = "orbtr"

// TestSpellingProbe_MeshShapedArrivalIsDenied covers the camelCase wire shape.
func TestSpellingProbe_MeshShapedArrivalIsDenied(t *testing.T) {
	r := newScopedRegistry()

	for _, scope := range []Scope{ScopePartition, ScopeWithinPartition, ScopePartition} {
		h := &stubMeta{name: "probe.meshArrival", scope: scope}
		err := r.validateScope(ctxWithRPC(map[string]interface{}{
			"tenantId": probeTenant, // CAMEL — what the wire actually stamps
		}), h)

		if err == nil {
			t.Errorf("scope %q: a mesh-shaped arrival was ADMITTED from a mutable "+
				"request-map key. Only the typed authenticated identity may satisfy "+
				"a scope tier: on an unbound (\"default\") transport the "+
				"transport/identity partition check is skipped, so this admission "+
				"would carry the SENDING APP'S constant, not the caller's partition", scope)
			continue
		}
		if !strings.Contains(err.Error(), "tenant") && !strings.Contains(err.Error(), "org") {
			t.Errorf("scope %q: denied, but not for the tenant-prerequisite reason "+
				"this probe characterises; got %v", scope, err)
		}
	}
}

// TestSpellingProbe_ReaderShapedArrivalIsDenied proves the snake_case spelling
// admits nothing either. Neither spelling is scope authority.
func TestSpellingProbe_ReaderShapedArrivalIsDenied(t *testing.T) {
	r := newScopedRegistry()

	for _, scope := range []Scope{ScopePartition, ScopeWithinPartition, ScopePartition} {
		h := &stubMeta{name: "probe.readerShape", scope: scope}
		err := r.validateScope(ctxWithRPC(map[string]interface{}{
			"tenant_id": probeTenant, // SNAKE — what the reader looks up
		}), h)
		if err == nil {
			t.Errorf("scope %q: mutable snake_case request hint manufactured identity", scope)
		}
	}
}

// TestSpellingProbe_TypedIdentityIsTheOnlyAdmissionPath locks the replacement
// boundary: map spelling is irrelevant, while the canonical typed identity
// admits the exact same platform partition.
func TestSpellingProbe_TypedIdentityIsTheOnlyAdmissionPath(t *testing.T) {
	r := newScopedRegistry()
	h := &stubMeta{name: "probe.control", scope: ScopePartition}

	camelErr := r.validateScope(ctxWithRPC(map[string]interface{}{"tenantId": probeTenant}), h)
	snakeErr := r.validateScope(ctxWithRPC(map[string]interface{}{"tenant_id": probeTenant}), h)

	if camelErr == nil || snakeErr == nil {
		t.Fatalf("both mutable spellings must deny (camel=%v snake=%v)", camelErr, snakeErr)
	}

	ctx := rpcscope.WithAuthenticatedIdentity(ctxWithRPC(map[string]interface{}{
		"tenantId":  probeTenant,
		"tenant_id": probeTenant,
	}), scopeIdentity(probeTenant, "", ""))
	if err := r.validateScope(ctx, h); err != nil {
		t.Errorf("typed authenticated platform tenant should admit: %v", err)
	}
}
