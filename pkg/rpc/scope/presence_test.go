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
	platform := NewIdentity("", "orbtr.ai")
	partition := NewIdentity("", "t1")
	full := NewIdentity("usr1", "t1")
	none := NewIdentity("", "")

	cases := []struct {
		name     string
		declared ScopeTag
		id       AuthenticatedIdentity
		want     bool
	}{
		{"none admits anyone", ScopeNone, none, true},
		{"allowlisted admits a listed partition", ScopeAllowlisted, platform, true},
		{"allowlisted denies an unlisted partition", ScopeAllowlisted, partition, false},
		{"allowlisted denies no partition", ScopeAllowlisted, none, false},
		{"partition needs a partition", ScopePartition, partition, true},
		{"partition denies no partition", ScopePartition, none, false},
		{"within-partition asks the same question as partition", ScopeWithinPartition, partition, true},
		{"within-partition denies no partition", ScopeWithinPartition, none, false},
		{"subject needs a partition and a subject", ScopeSubject, full, true},
		{"subject denies a partition with no subject", ScopeSubject, partition, false},
		{"subject denies a subject with no partition", ScopeSubject, NewIdentity("usr1", ""), false},
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
	if ok, _ := p.CheckPresence(ScopeAllowlisted, NewIdentity("", "anything")); ok {
		t.Fatal("empty allowlist must never satisfy ScopeAllowlisted")
	}
	// The other tiers still turn on the identity's partition.
	if ok, _ := p.CheckPresence(ScopePartition, NewIdentity("", "t1")); !ok {
		t.Fatal("ScopePartition turns on the identity's partition, independent of the platform allowlist")
	}
}

// TestScopePresenceTurnsOnThePartitionNotOnALevelOrder records what the policy
// keys on, because the tag NAMES read as a hierarchy of levels and the mechanism
// is not one. Every tag asks about the partition the identity carries;
// ScopeSubject additionally asks for a subject, and ScopeAllowlisted asks which
// partition it is. A reader who assumes an order would expect a partition alone
// to satisfy ScopeSubject, or an ordinary partition to satisfy ScopeAllowlisted.
func TestScopePresenceTurnsOnThePartitionNotOnALevelOrder(t *testing.T) {
	p := NewScopePresencePolicy("plat")

	// A partition alone does not supply a subject.
	partitionOnly := NewIdentity("", "t1")
	if ok, _ := p.CheckPresence(ScopePartition, partitionOnly); !ok {
		t.Fatal("precondition: a partition satisfies ScopePartition")
	}
	if ok, _ := p.CheckPresence(ScopeSubject, partitionOnly); ok {
		t.Error("an identity satisfying ScopePartition also satisfied ScopeSubject — ScopeSubject asks for a subject the partition does not supply")
	}

	// A subject alone does not supply a partition, so it satisfies no tier
	// above ScopeNone.
	subjectOnly := NewIdentity("usr1", "")
	if ok, _ := p.CheckPresence(ScopeSubject, subjectOnly); ok {
		t.Error("a subject with no partition satisfied ScopeSubject")
	}
	if ok, _ := p.CheckPresence(ScopePartition, subjectOnly); ok {
		t.Error("a subject with no partition satisfied ScopePartition")
	}

	// ScopeWithinPartition and ScopePartition ask the one question an identity
	// can answer — whether it carries a partition. Which partition a handler
	// may act on is the handler's own selector, not a tier, so the two tags do
	// not separate.
	if okP, _ := p.CheckPresence(ScopePartition, partitionOnly); !okP {
		t.Fatal("precondition: ScopePartition admits a partition")
	}
	if okW, _ := p.CheckPresence(ScopeWithinPartition, partitionOnly); !okW {
		t.Error("ScopeWithinPartition refused an identity ScopePartition admitted — the two tags ask the same question")
	}

	// The platform allowlist is a membership test, not a tier above the others:
	// an ordinary partition satisfies ScopePartition and must still be refused
	// ScopeAllowlisted.
	if ok, _ := p.CheckPresence(ScopeAllowlisted, partitionOnly); ok {
		t.Error("a partition outside the allowlist satisfied ScopeAllowlisted")
	}
	if ok, _ := p.CheckPresence(ScopeAllowlisted, NewIdentity("", "plat")); !ok {
		t.Error("the allowlisted partition was refused ScopeAllowlisted")
	}
}
