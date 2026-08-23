/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// COVERAGE of the Runtime's outward-facing reach setters:
// SetRoles (:35), SetServiceName (:63), rolesJoined (:73) — all at 0.0%.
//
// 🛑 SetCapabilityMetadata (:54) IS DELIBERATELY NOT TESTED HERE. It is the
// whole of another writer's uncommitted hunk in this file (`@@ -42,0 +43,17 @@`,
// the SetCapabilityExtras work). Pinning a design someone is
// mid-authoring would freeze it, and their next edit would surface as my test
// breaking. Ownership census run BEFORE writing, not after.
//
// 🔑 WHY THESE THREE MATTER: they are how something OUTSIDE the mesh changes what
// this node advertises. The doc names the callers — "AI/Quorum shim, runtime
// promotion logic" — so they run on a live node mid-flight, and a panic or a
// retained slice here corrupts what every peer believes about this node.

// 🔴 THE COPY IS THE PROPERTY. SetRoles stores `append([]string(nil), roles...)`,
// so a caller that reuses or mutates its slice afterwards cannot silently
// re-advertise this node with roles it never chose.
func TestSetRolesDoesNotRetainTheCallersSliceInConfig(t *testing.T) {
	rt := &Runtime{}
	in := []string{"anchor", "auth"}

	rt.SetRoles(in)
	in[0] = "attacker-injected"

	for _, r := range rt.cfg.Roles {
		if r == "attacker-injected" {
			t.Fatal("mutating the caller's slice changed rt.cfg.Roles — SetRoles " +
				"retained the caller's array, so any later write by the caller " +
				"changes what this node advertises without going through SetRoles")
		}
	}
	if len(rt.cfg.Roles) != 2 {
		t.Fatalf("cfg.Roles = %v, want the 2 roles that were set", rt.cfg.Roles)
	}
}

// 🔴 AND IT MUST NOT PANIC WITH NO SWARM. The doc's named callers are external
// (the AI/Quorum shim, promotion logic) and run against a live Runtime — but a
// Runtime whose swarm or publisher is absent is the ordinary pre-Initialize and
// minimal-build shape. A panic here takes down the caller, not the mesh.
func TestTheReachSettersTolerateAnAbsentSwarm(t *testing.T) {
	rt := &Runtime{} // swarm nil

	rt.SetRoles([]string{"anchor"})
	rt.SetServiceName("help-orbtr-io")

	if got := rt.cfg.ServiceName; got != "help-orbtr-io" {
		t.Fatalf("ServiceName = %q — the local config must be updated even when "+
			"there is no publisher to tell, or a later Initialize publishes the "+
			"stale name", got)
	}
	if len(rt.cfg.Roles) != 1 {
		t.Fatalf("cfg.Roles = %v, want the role that was set", rt.cfg.Roles)
	}
}

// SetServiceName is documented as rare ("A/B blue-green rename") and must
// actually replace, not append or merge — a node advertising two service names
// is routable as both.
func TestSetServiceNameReplacesRatherThanAccumulates(t *testing.T) {
	rt := &Runtime{}

	rt.SetServiceName("first")
	rt.SetServiceName("second")

	if rt.cfg.ServiceName != "second" {
		t.Fatalf("ServiceName = %q after two sets, want %q", rt.cfg.ServiceName, "second")
	}
}

// rolesJoined feeds legacy log lines. The only things worth pinning are the
// separator (log scrapers split on it) and that the empty set does not become
// a phantom empty role.
func TestRolesJoinedUsesCommasAndYieldsEmptyForNoRoles(t *testing.T) {
	if got := rolesJoined([]string{"anchor", "auth", "billing"}); got != "anchor,auth,billing" {
		t.Fatalf("rolesJoined = %q, want comma-separated with no spaces — the log "+
			"scrapers split on the separator", got)
	}
	if got := rolesJoined(nil); got != "" {
		t.Fatalf("rolesJoined(nil) = %q, want empty — a separator with nothing "+
			"around it reads as a role named \"\"", got)
	}
	if got := rolesJoined([]string{"solo"}); got != "solo" {
		t.Fatalf("rolesJoined([solo]) = %q, want no trailing separator", got)
	}
}

// meshHTTPPort is the fallback a peer applies when a reach record predates the
// http_port attribute. The default is not arbitrary — every fleet fly.toml binds
// internal_port 8080 — so a change here silently misdirects the WS dial of every
// peer reading an older record.
func TestMeshHTTPPortFallsBackToTheFleetInternalPort(t *testing.T) {
	// ⚠ THE LITERAL, DELIBERATELY — not `defaultMeshHTTPPort`. My first version
	// compared meshHTTPPort() against the constant itself, which is tautological:
	// both move together, so a mutant changing 8080→8081 SURVIVED. The claim this
	// test makes is that the value is 8080 *because every fleet fly.toml binds
	// internal_port 8080*, and only the literal can carry that claim.
	t.Setenv("PORT", "")
	if got := meshHTTPPort(); got != "8080" {
		t.Fatalf("meshHTTPPort() = %q with no $PORT, want \"8080\" — every fleet "+
			"fly.toml binds internal_port 8080, so this fallback is what a peer "+
			"dials when the reach record has no http_port. Changing it misdirects "+
			"the WS dial of every peer reading an older record", got)
	}

	t.Setenv("PORT", "9999")
	if got := meshHTTPPort(); got != "9999" {
		t.Fatalf("meshHTTPPort() = %q with $PORT=9999 — the env var is the source "+
			"of truth and is being ignored", got)
	}
}
