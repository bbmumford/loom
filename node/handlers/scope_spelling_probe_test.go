/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"strings"
	"testing"
)

// The behavioural probe R-959 requires before the tenant-key spelling fix lands.
//
// BACKGROUND, all measured: the mesh wire stamps the tenant under CAMEL
// "tenantId" (loom pkg/dispatch/hwp_dispatch.go, guarded on a non-empty
// platformTenant which 8 endpoints set at startup). node/rpc.go executeLocal
// copies req.Context key-for-key into handlers.RPCRequest.Context, and
// Dispatch's injectRPCContext merges that map under "rpc_context" verbatim. So
// by the time validateTenantScope runs, the camel key is present in the map it
// reads — and GetTenantIDFromContext looks up SNAKE "tenant_id" and misses.
//
// ⚠ THIS IS A CHARACTERISATION TEST, NOT AN APPROVAL. It asserts what the code
// does today. Neither outcome it pins is desirable:
//
//   - the mesh-shaped arrival is DENIED, but for a key-not-found reason rather
//     than an authorization decision (an accidental barrier);
//   - the moment the lookup finds a value, the arm ADMITS — and on the mesh the
//     value available is the SENDING APP'S platform constant, not the caller's
//     tenant.
//
// ⇒ so "fixing the spelling" converts a denial into an admission under a shared
// constant. On endpoints running the single-tenant fallback the transport/request
// tenant-match check is a deliberate no-op ("default" transport allows any
// request tenant), so nothing downstream catches it there.
//
// 🛑 IF TestSpellingProbe_MeshShapedArrivalIsDenied STARTS FAILING, the spelling
// was changed. That is not automatically wrong — but it must not land before the
// deliberate gate it replaces is standing (a roster check on the accepting side;
// aether's VerifyNodeInfo already takes an `expected` NodeID that the responder
// currently passes empty). Read the failure as "the barrier just moved", and
// confirm the substitution was addressed first.
const probeTenant = "orbtr"

// TestSpellingProbe_MeshShapedArrivalIsDenied is the BEFORE half.
//
// The context is built exactly as the mesh produces it: the camel key the wire
// stamps, and no snake key, because nothing in loom's non-test code writes one.
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

// TestSpellingProbe_ReaderShapedArrivalIsAdmitted is the AFTER half — it
// simulates the post-fix state without changing production code.
//
// Same value, spelled the way GetTenantIDFromContext reads it. If this admits
// while the camel case above denies, the gate's outcome is decided by SPELLING
// and not by any property of the caller. That pair is the whole finding.
func TestSpellingProbe_ReaderShapedArrivalIsAdmitted(t *testing.T) {
	r := NewHandlerRegistry()

	for _, scope := range []TenantScope{TenantScopeTenant, TenantScopeOrg, TenantScopeProfile} {
		h := &stubMeta{name: "probe.readerShape", scope: scope}
		if err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
			"tenant_id": probeTenant, // SNAKE — what the reader looks up
		}), h); err != nil {
			t.Errorf("scope %q: the gate REFUSED a tenant it could see (%v). If this "+
				"fails, the spelling is no longer the deciding factor and the "+
				"companion probe's premise needs re-measuring", scope, err)
		}
	}
}

// TestSpellingProbe_SpellingIsTheOnlyDifference is the control that makes the
// pair above admissible rather than two unrelated observations.
//
// It asserts the two contexts differ ONLY in the key's spelling — same value,
// same scope, same registry — so the opposite outcomes cannot be attributed to
// anything else. Without this, the pair proves nothing about causation.
func TestSpellingProbe_SpellingIsTheOnlyDifference(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "probe.control", scope: TenantScopeTenant}

	camelErr := r.validateTenantScope(ctxWithRPC(map[string]interface{}{"tenantId": probeTenant}), h)
	snakeErr := r.validateTenantScope(ctxWithRPC(map[string]interface{}{"tenant_id": probeTenant}), h)

	if camelErr == nil || snakeErr != nil {
		t.Fatalf("control failed — the probe pair is only meaningful while camel "+
			"denies and snake admits (camel=%v snake=%v)", camelErr, snakeErr)
	}

	// And both spellings present together — what a dual-read or canonicalising
	// fix would produce. Documented so the intended end state is pinned too.
	if err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
		"tenantId":  probeTenant,
		"tenant_id": probeTenant,
	}), h); err != nil {
		t.Errorf("with both spellings present the gate should resolve the tenant; got %v", err)
	}
}
