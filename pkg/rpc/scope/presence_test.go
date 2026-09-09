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
		{"platform needs allowlisted pinned", ScopeAllowlisted, platform, true},
		{"platform denies non-allowlisted", ScopeAllowlisted, partition, false},
		{"platform denies empty", ScopeAllowlisted, none, false},
		{"tenant needs pinned", ScopePartition, partition, true},
		{"tenant denies empty", ScopePartition, none, false},
		{"partition (neutral) needs pinned", ScopePartition, partition, true},
		{"org needs pinned+within", ScopeWithinPartition, full, true},
		{"org denies pinned-only", ScopeWithinPartition, partition, false},
		{"user needs pinned+subject", ScopeSubject, full, true},
		{"user denies no-subject", ScopeSubject, partition, false},
		{"profile needs pinned", ScopePartition, partition, true},
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
	if ok, _ := p.CheckPresence(ScopeAllowlisted, NewScopeIdentity("anything", "", "")); ok {
		t.Fatal("empty allowlist must never satisfy ScopeAllowlisted")
	}
	// Non-platform tiers still turn on axis presence.
	if ok, _ := p.CheckPresence(ScopePartition, NewScopeIdentity("t1", "", "")); !ok {
		t.Fatal("tenant tier turns on pinned-axis presence, independent of the platform allowlist")
	}
}

// TestScopePresenceTurnsOnAxisPresenceNotOnALevelOrder records what the policy
// actually keys on, because the tag NAMES read as a hierarchy and the mechanism
// is not one: each tag names which axes must be PRESENT, and no tag subsumes
// another. A reader who assumes an order would expect ScopeSubject to satisfy
// ScopeWithinPartition's requirement, and it does not.
func TestScopePresenceTurnsOnAxisPresenceNotOnALevelOrder(t *testing.T) {
	p := NewScopePresencePolicy("plat")

	pinnedSubject := NewScopeIdentity("t1", "", "usr1")
	if ok, _ := p.CheckPresence(ScopeSubject, pinnedSubject); !ok {
		t.Fatal("precondition: pinned+subject satisfies ScopeSubject")
	}
	if ok, _ := p.CheckPresence(ScopeWithinPartition, pinnedSubject); ok {
		t.Error("an identity satisfying ScopeSubject also satisfied ScopeWithinPartition — the tags are not ordered, and ScopeWithinPartition turns on the within-transport axis a subject does not supply")
	}

	pinnedWithin := NewScopeIdentity("t1", "org1", "")
	if ok, _ := p.CheckPresence(ScopeWithinPartition, pinnedWithin); !ok {
		t.Fatal("precondition: pinned+within satisfies ScopeWithinPartition")
	}
	if ok, _ := p.CheckPresence(ScopeSubject, pinnedWithin); ok {
		t.Error("an identity satisfying ScopeWithinPartition also satisfied ScopeSubject — ScopeSubject turns on a subject, which the within-transport axis does not supply")
	}

	// The platform allowlist is a membership test, not a tier above the others:
	// an ordinary pinned identity satisfies ScopePartition and must still be refused
	// ScopeAllowlisted.
	ordinary := NewScopeIdentity("t1", "", "")
	if ok, _ := p.CheckPresence(ScopePartition, ordinary); !ok {
		t.Fatal("precondition: pinned satisfies ScopePartition")
	}
	if ok, _ := p.CheckPresence(ScopeAllowlisted, ordinary); ok {
		t.Error("a non-allowlisted pinned identity satisfied ScopeAllowlisted")
	}
}
