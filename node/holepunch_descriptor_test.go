/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"testing"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/nat"
	"github.com/bbmumford/loom/node/handlers"
)

// Covers the hole-punch RPC handler's AUTHORIZATION DESCRIPTOR and the
// dial-path entry point:
//
//	Role · RequiresAuth · AllowedAuthTypes · Scopes · TenantScope · AllowedTenants  (:211-:216)
//	attemptHolePunch (:268)
//
// Censused per symbol, one level out, including interface satisfaction. These
// six are METHODS SATISFYING handlers.SecureHandler, so nothing calls them
// by name; the registry calls them through the interface:
//
//	registered  runtime.go:591  registry.RegisterRPC(&holepunchRPCHandler{...})
//	local gate  internal/securityctx/securityctx.go:99  if !h.RequiresAuth() { return nil }
//	🔴 GOSSIPED  runtime.go:3025  PublishHandlersToLAD -> lad.HandlerMetadata{RequiresAuth: ...}
//
// 🔑 SO THE DESCRIPTOR IS NOT ONLY A LOCAL GATE INPUT — IT IS ADVERTISED TO EVERY
// PEER in this node's LAD role record. A wrong value is both a local
// authorization decision and a false statement to the whole mesh.
//
// ✅ AND THE DESIGN'S PREMISE WAS CHECKED, NOT ASSUMED. `RequiresAuth() == false`
// rests on "the mesh RPC path rides an authenticated session, so the session
// handshake is the security boundary" (stated three times in holepunch.go). That
// premise needs the auth machinery to be real, and it is: endpoints install
// `helpers.LoomValidator{}` into Config.AuthValidator — 2 assignments in the
// ORBTR root, 11 in HSTLES — so securityctx.Default()'s fail-closed loom-local
// validator is a fallback, not what production runs.

// punchCoordForTest mirrors how holepunch_test.go builds a coordinator inline;
// there is no shared fixture in that file to reuse.
func punchCoordForTest(t *testing.T) *punchCoordinator {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	eim := nat.NATBehaviour{
		Mapping:   nat.MappingEndpointIndependent,
		Filtering: nat.FilteringEndpointIndependent,
	}
	addrs := []net.UDPAddr{{IP: net.IPv4(1, 2, 3, 4), Port: 41641}}
	return newPunchCoordinator(id, priv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return addrs })
}

// punchRequestForTest is a well-formed inbound request from some other node.
func punchRequestForTest(t *testing.T) *nat.PunchRequest {
	t.Helper()
	return punchCoordForTest(t).buildRequest(aether.NodeID("target"))
}

func punchHandlerForTest(t *testing.T) *holepunchRPCHandler {
	t.Helper()
	return &holepunchRPCHandler{coord: punchCoordForTest(t), probe: nil}
}

// 🔴 THE DESCRIPTOR IS A PUBLISHED CONTRACT. Each value below is copied into
// lad.HandlerMetadata and gossiped, so this test states what this node tells the
// mesh about the holepunch handler.
func TestTheHolePunchDescriptorIsTheContractItGossips(t *testing.T) {
	h := punchHandlerForTest(t)

	if got := h.Name(); got != holepunchRPCHandlerName {
		t.Fatalf("Name() = %q, want %q — peers invoke by this exact string "+
			"(requestOffer sets RPCRequest.Handler from the same constant)",
			got, holepunchRPCHandlerName)
	}
	if got := h.Role(); got != "system" {
		t.Fatalf("Role() = %q, want \"system\" — the handler is infrastructure, "+
			"not a tenant-facing operation", got)
	}
}

// 🔴 RequiresAuth() == false IS DELIBERATE, AND THIS TEST RECORDS WHY SO THAT
// "HARDENING" IT IS A DELIBERATE ACT RATHER THAN AN OBVIOUS-LOOKING WIN.
//
// The reason, stated three times in holepunch.go: a peer invokes this over its
// EXISTING authenticated mesh session, so the session handshake is the security
// boundary and a per-RPC check would be redundant. holepunch.go:102 also
// disposes of the obvious attack: "A forged RequesterNodeID is not an
// escalation: the offer is simply addressed to the wrong node and the spoofer's
// own punch fails."
//
// ⚠ AND THE COST OF FLIPPING IT: securityctx's validator requires an
// authenticated principal in the ctx, and nothing writes that key on the mesh
// RPC path (securityctx.WithAuth has no non-test caller in loom — note the
// same-named WithAuth methods in node/handlers/ are unrelated BUILDER setters).
// So `true` here would not harden NAT traversal, it would DENY every punch
// request and silently disable hole-punching fleet-wide.
func TestHolePunchDeliberatelyRequiresNoPerRPCAuth(t *testing.T) {
	h := punchHandlerForTest(t)

	if h.RequiresAuth() {
		t.Fatal("RequiresAuth() is now true. If that is intentional, a principal " +
			"must first be written into the mesh RPC context — securityctx's " +
			"validator denies every RequiresAuth handler when nothing " +
			"authenticates, so this change DISABLES hole punching rather than " +
			"securing it. See holepunch.go's descriptor.")
	}
}

// 🔑 THE CONSISTENCY PROPERTY, AND IT IS THE ONE WORTH HAVING. A handler that
// requires no auth must not ALSO declare scopes, auth types or tenant
// restrictions: securityctx returns at `if !h.RequiresAuth()` before reading any
// of them, so such declarations are unenforceable — and worse, they are
// published to LAD as requirements that nothing checks. Every peer would read a
// restriction this node does not apply.
func TestAnUnauthenticatedHandlerDeclaresNoUnenforceableRestrictions(t *testing.T) {
	h := punchHandlerForTest(t)

	if h.RequiresAuth() {
		t.Skip("premise changed: see TestHolePunchDeliberatelyRequiresNoPerRPCAuth")
	}
	if got := h.AllowedAuthTypes(); len(got) != 0 {
		t.Errorf("AllowedAuthTypes() = %v on a no-auth handler — securityctx "+
			"returns before reading it, so this is published to LAD as a "+
			"requirement nothing enforces", got)
	}
	if got := h.Scopes(); len(got) != 0 {
		t.Errorf("Scopes() = %v on a no-auth handler — PublishHandlersToLAD copies "+
			"this into RequiredScopes and gossips it, so every peer reads a scope "+
			"requirement this node never checks", got)
	}
	if got := h.AllowedTenants(); len(got) != 0 {
		t.Errorf("AllowedTenants() = %v on a no-auth handler — an unenforceable "+
			"tenant allowlist", got)
	}
	if got := h.TenantScope(); got != handlers.TenantScopeNone {
		t.Errorf("TenantScope() = %q on a no-auth handler — tenant scoping cannot "+
			"be applied without an authenticated principal to scope", got)
	}
}

// ── ExecuteRPC's rejection paths ────────────────────────────────────────────

// Malformed input must yield an UNSUCCESSFUL RESPONSE, not a transport error.
// The distinction matters: returning an error propagates as an RPC-layer
// failure, which the caller records against the peer's dial history — so a
// garbage payload from one peer would look like a broken path.
func TestAMalformedPunchRequestIsRejectedInBandNotAsATransportError(t *testing.T) {
	h := punchHandlerForTest(t)

	resp, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{
		ID: "hp-1", Payload: []byte("{not json"),
	})
	if err != nil {
		t.Fatalf("ExecuteRPC returned a transport error %v for a malformed "+
			"payload — the caller reads that as a broken session rather than a "+
			"bad request", err)
	}
	if resp == nil {
		t.Fatal("nil response and nil error — the caller has nothing to read")
	}
	if resp.Success {
		t.Fatal("a malformed PunchRequest was accepted as successful")
	}
	if resp.ID != "hp-1" {
		t.Errorf("response ID = %q, want the request's %q — a mismatched ID "+
			"cannot be correlated to its pending call", resp.ID, "hp-1")
	}
	if resp.Error == "" {
		t.Error("an unsuccessful response carries no error text — the peer cannot " +
			"tell why it was rejected")
	}
}

// A well-formed request with no probe wired is signaling-only and must still
// return a decodable offer: that is the documented test/fallback shape.
func TestAWellFormedRequestYieldsADecodableOfferWithNoProbeWired(t *testing.T) {
	h := punchHandlerForTest(t)

	payload, err := json.Marshal(punchRequestForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{
		ID: "hp-2", Payload: payload,
	})
	if err != nil {
		t.Fatalf("ExecuteRPC error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("a well-formed request was rejected: %s", resp.Error)
	}
	if len(resp.Payload) == 0 {
		t.Fatal("successful response carries no offer payload — the initiator has " +
			"no candidates to punch toward")
	}
}

// ── The dial-path entry point ───────────────────────────────────────────────

// attemptHolePunch runs on the FAILED-DIAL path in multipath_dial.go, beside
// the recordDialFailure site. A node with no punch
// coordinator must decline cleanly and let the caller keep the peer's existing
// path — never panic, because this runs precisely when things are already going
// wrong.
func TestAHolePunchAttemptWithNoCoordinatorDeclinesCleanly(t *testing.T) {
	m := registerTestManager()
	m.rt = &Runtime{} // punchCoord deliberately nil

	if m.attemptHolePunch(context.Background(), testNodeIDA, "syd") {
		t.Fatal("attemptHolePunch reported success with no punch coordinator — " +
			"the caller will treat a peer as reachable over a path that was never " +
			"established")
	}
}
