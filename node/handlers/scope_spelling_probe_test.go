/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"strings"
	"testing"

	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// These probes began as a characterisation of the camelCase/snake_case split
// in mutable RPC maps. The replacement contract makes spelling irrelevant:
// neither map is authority. Only the typed server-established identity may
// satisfy a scope tier.
const probeTenant = "orbtr"

// TestSpellingProbe_MeshShapedArrivalIsDenied covers the camelCase wire shape.
func TestSpellingProbe_MeshShapedArrivalIsDenied(t *testing.T) {
	r := NewHandlerRegistry()

	for _, scope := range []TenantScope{TenantScopeTenant, TenantScopeOrg, TenantScopeProfile} {
		h := &stubMeta{name: "probe.meshArrival", scope: scope}
		err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
			"tenantId": probeTenant, // CAMEL — what the wire actually stamps
		}), h)

		if err == nil {
			t.Errorf("scope %q: a mesh-shaped arrival was ADMITTED. The tenant-key "+
				"spelling appears to have been reconciled. That is the change R-959 "+
				"required a probe for: verify the identity-substitution half was "+
				"addressed FIRST, because on single-tenant-fallback endpoints the "+
				"transport/request tenant-match check is a no-op and this admission "+
				"carries the SENDING APP'S constant, not the caller's tenant", scope)
			continue
		}
		if !strings.Contains(err.Error(), "tenant") && !strings.Contains(err.Error(), "org") {
			t.Errorf("scope %q: denied, but not for the tenant-prerequisite reason "+
				"this probe characterises; got %v", scope, err)
		}
	}
}

// TestSpellingProbe_ReaderShapedArrivalIsDenied proves the old snake_case
// accidental admission is gone. Neither spelling is scope authority.
func TestSpellingProbe_ReaderShapedArrivalIsDenied(t *testing.T) {
	r := NewHandlerRegistry()

	for _, scope := range []TenantScope{TenantScopeTenant, TenantScopeOrg, TenantScopeProfile} {
		h := &stubMeta{name: "probe.readerShape", scope: scope}
		err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
			"tenant_id": probeTenant, // SNAKE — what the reader looks up
		}), h)
		if err == nil {
			t.Errorf("scope %q: mutable snake_case request hint manufactured identity", scope)
		}
	}
}

// TestSpellingProbe_TypedIdentityIsTheOnlyAdmissionPath locks the replacement
// boundary: map spelling is irrelevant, while the canonical typed identity
// admits the exact same platform tenant.
func TestSpellingProbe_TypedIdentityIsTheOnlyAdmissionPath(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "probe.control", scope: TenantScopeTenant}

	camelErr := r.validateTenantScope(ctxWithRPC(map[string]interface{}{"tenantId": probeTenant}), h)
	snakeErr := r.validateTenantScope(ctxWithRPC(map[string]interface{}{"tenant_id": probeTenant}), h)

	if camelErr == nil || snakeErr == nil {
		t.Fatalf("both mutable spellings must deny (camel=%v snake=%v)", camelErr, snakeErr)
	}

	ctx := tenantScope.WithAuthenticatedIdentity(ctxWithRPC(map[string]interface{}{
		"tenantId":  probeTenant,
		"tenant_id": probeTenant,
	}), tenantScope.AuthenticatedIdentity{PlatformTenantID: probeTenant})
	if err := r.validateTenantScope(ctx, h); err != nil {
		t.Errorf("typed authenticated platform tenant should admit: %v", err)
	}
}
