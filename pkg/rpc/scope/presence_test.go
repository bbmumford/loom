package scope

import "testing"

func TestNewScopeIdentity_AxesAndPartition(t *testing.T) {
	id := NewScopeIdentity("pt1", "org1", "usr1")
	if got := id.Scope(AxisTransportPinned); got != "pt1" {
		t.Fatalf("transport-pinned axis: got %q want pt1", got)
	}
	if got := id.Scope(AxisWithinTransport); got != "org1" {
		t.Fatalf("within-transport axis: got %q want org1", got)
	}
	if id.Subject != "usr1" {
		t.Fatalf("subject: got %q want usr1", id.Subject)
	}
	if id.Partition() != "org1" {
		t.Fatalf("partition: got %q want org1 (within-transport axis)", id.Partition())
	}
	// An omitted axis stays absent (empty string == omitted through Scope).
	bare := NewScopeIdentity("", "", "")
	if bare.Scope(AxisTransportPinned) != "" || bare.Partition() != "" {
		t.Fatal("all-empty identity must carry no axes and no partition")
	}
}

func TestNewScopePresencePolicy_Tiers(t *testing.T) {
	p := NewScopePresencePolicy("orbtr.ai")
	platform := NewScopeIdentity("orbtr.ai", "", "")
	partition := NewScopeIdentity("t1", "", "")
	full := NewScopeIdentity("t1", "org1", "usr1")
	none := NewScopeIdentity("", "", "")

	cases := []struct {
		name     string
		declared ScopeTag
		id       AuthenticatedIdentity
		want     bool
	}{
		{"none admits anyone", ScopeNone, none, true},
		{"platform needs allowlisted pinned", ScopePlatform, platform, true},
		{"platform denies non-allowlisted", ScopePlatform, partition, false},
		{"platform denies empty", ScopePlatform, none, false},
		{"tenant needs pinned", ScopeTenant, partition, true},
		{"tenant denies empty", ScopeTenant, none, false},
		{"partition (neutral) needs pinned", ScopePartition, partition, true},
		{"org needs pinned+within", ScopeOrg, full, true},
		{"org denies pinned-only", ScopeOrg, partition, false},
		{"user needs pinned+subject", ScopeUser, full, true},
		{"user denies no-subject", ScopeUser, partition, false},
		{"profile needs pinned", ScopeProfile, partition, true},
		{"unknown fails closed", ScopeUnknown, full, false},
	}
	for _, c := range cases {
		if ok, reason := p.CheckPresence(c.declared, c.id); ok != c.want {
			t.Errorf("%s: got %v (%q) want %v", c.name, ok, reason, c.want)
		}
	}
}

func TestNewScopePresencePolicy_EmptyAllowlistFailsClosedPlatform(t *testing.T) {
	p := NewScopePresencePolicy() // no platform partitions
	if ok, _ := p.CheckPresence(ScopePlatform, NewScopeIdentity("anything", "", "")); ok {
		t.Fatal("empty allowlist must never satisfy ScopePlatform")
	}
	// Non-platform tiers still turn on axis presence.
	if ok, _ := p.CheckPresence(ScopeTenant, NewScopeIdentity("t1", "", "")); !ok {
		t.Fatal("tenant tier turns on pinned-axis presence, independent of the platform allowlist")
	}
}

// TestScopeTenantAndScopePartitionAreIndistinguishable pins the property a
// consumer's rename depends on: ScopePartition is the neutral name for what
// ScopeTenant already required, so swapping a handler's declaration from one to
// the other must change nothing this policy decides.
//
// The tier table above exercises each tag against a single identity, which
// passes even if the two are given separate arms that agree only there. This
// compares them across every identity shape and asserts the REASON as well, so a
// split that changes either the verdict or the diagnosis is caught. Without it a
// rename across the consumer fleet could alter dispatch admission with nothing
// failing.
func TestScopeTenantAndScopePartitionAreIndistinguishable(t *testing.T) {
	p := NewScopePresencePolicy("plat")

	ids := []struct {
		name string
		id   AuthenticatedIdentity
	}{
		{"empty", NewScopeIdentity("", "", "")},
		{"pinned only", NewScopeIdentity("t1", "", "")},
		{"pinned+within", NewScopeIdentity("t1", "org1", "")},
		{"pinned+subject", NewScopeIdentity("t1", "", "usr1")},
		{"full", NewScopeIdentity("t1", "org1", "usr1")},
		{"allowlisted platform", NewScopeIdentity("plat", "", "")},
		{"within only, no pinned", NewScopeIdentity("", "org1", "")},
		{"subject only, no pinned", NewScopeIdentity("", "", "usr1")},
	}

	for _, c := range ids {
		tenantOK, tenantWhy := p.CheckPresence(ScopeTenant, c.id)
		partOK, partWhy := p.CheckPresence(ScopePartition, c.id)
		if tenantOK != partOK {
			t.Errorf("%s: ScopeTenant=%v but ScopePartition=%v — the neutral name is no longer a synonym, so renaming a handler changes what it admits",
				c.name, tenantOK, partOK)
		}
		if tenantWhy != partWhy {
			t.Errorf("%s: denial reasons diverged — ScopeTenant %q vs ScopePartition %q",
				c.name, tenantWhy, partWhy)
		}
	}
}

// TestScopePresenceTurnsOnAxisPresenceNotOnALevelOrder records what the policy
// actually keys on, because the tag NAMES read as a hierarchy and the mechanism
// is not one: each tag names which axes must be PRESENT, and no tag subsumes
// another. A reader who assumes an order would expect ScopeUser to satisfy
// ScopeOrg's requirement, and it does not.
func TestScopePresenceTurnsOnAxisPresenceNotOnALevelOrder(t *testing.T) {
	p := NewScopePresencePolicy("plat")

	pinnedSubject := NewScopeIdentity("t1", "", "usr1")
	if ok, _ := p.CheckPresence(ScopeUser, pinnedSubject); !ok {
		t.Fatal("precondition: pinned+subject satisfies ScopeUser")
	}
	if ok, _ := p.CheckPresence(ScopeOrg, pinnedSubject); ok {
		t.Error("an identity satisfying ScopeUser also satisfied ScopeOrg — the tags are not ordered, and ScopeOrg turns on the within-transport axis a subject does not supply")
	}

	pinnedWithin := NewScopeIdentity("t1", "org1", "")
	if ok, _ := p.CheckPresence(ScopeOrg, pinnedWithin); !ok {
		t.Fatal("precondition: pinned+within satisfies ScopeOrg")
	}
	if ok, _ := p.CheckPresence(ScopeUser, pinnedWithin); ok {
		t.Error("an identity satisfying ScopeOrg also satisfied ScopeUser — ScopeUser turns on a subject, which the within-transport axis does not supply")
	}

	// The platform allowlist is a membership test, not a tier above the others:
	// an ordinary pinned identity satisfies ScopeTenant and must still be refused
	// ScopePlatform.
	ordinary := NewScopeIdentity("t1", "", "")
	if ok, _ := p.CheckPresence(ScopeTenant, ordinary); !ok {
		t.Fatal("precondition: pinned satisfies ScopeTenant")
	}
	if ok, _ := p.CheckPresence(ScopePlatform, ordinary); ok {
		t.Error("a non-allowlisted pinned identity satisfied ScopePlatform")
	}
}
