package scope

import "fmt"

// scopePresencePolicy is the reference PresencePolicy: it derives a tier verdict
// for a declared ScopeTag from the opaque identity's partition and subject. loom
// ships no policy by default — a registry with a nil policy fails every non-None
// handler CLOSED — so this is the canonical implementation a consumer installs
// via Registry.SetPresencePolicy when it wants tier-presence enforcement, in
// place of writing its own.
//
// Every tier turns on the partition the identity carries, so a handler declaring
// ScopePartition admits exactly a caller that has one, and platformAllow names
// the partitions ScopeAllowlisted admits. A caller whose partition is empty
// reaches no handler above ScopeNone, which is the deny a store selector keyed
// on that partition would otherwise have to make after the handler had already
// been entered.
type scopePresencePolicy struct {
	platformAllow map[string]struct{}
}

// NewScopePresencePolicy builds the reference PresencePolicy whose
// ScopeAllowlisted tag admits exactly the supplied partitions. Empty strings are
// dropped, so an empty argument list yields a policy where ScopeAllowlisted is
// never satisfied (fail-closed) while the other tags still turn on the
// identity's partition.
func NewScopePresencePolicy(platformPartitions ...string) PresencePolicy {
	allow := make(map[string]struct{}, len(platformPartitions))
	for _, p := range platformPartitions {
		if p != "" {
			allow[p] = struct{}{}
		}
	}
	return scopePresencePolicy{platformAllow: allow}
}

// isPlatform reports whether v is a non-empty member of the platform allowlist.
// An empty value, or one absent from the allowlist, fails closed.
func (p scopePresencePolicy) isPlatform(v string) bool {
	if v == "" {
		return false
	}
	_, ok := p.platformAllow[v]
	return ok
}

// CheckPresence reports whether the opaque identity satisfies the declared tag.
// The default arm fails CLOSED so an unrecognised or sentinel tag (ScopeUnknown)
// never decays to admission.
func (p scopePresencePolicy) CheckPresence(declared ScopeTag, id AuthenticatedIdentity) (bool, string) {
	partition := id.Partition()
	subj := id.Subject
	switch declared {
	case ScopeNone:
		return true, ""
	case ScopeAllowlisted:
		if p.isPlatform(partition) {
			return true, ""
		}
		return false, "requires a partition the policy admits"
	case ScopePartition, ScopeWithinPartition:
		// Both tags ask the one question the identity can answer: does the
		// caller carry a partition. Which partition a handler may act on is the
		// handler's own selector, not a tier.
		if partition != "" {
			return true, ""
		}
		return false, "requires an authenticated partition"
	case ScopeSubject:
		if partition == "" {
			return false, "requires an authenticated partition"
		}
		if subj == "" {
			return false, "requires an authenticated subject"
		}
		return true, ""
	default:
		return false, fmt.Sprintf("unknown scope %q", declared)
	}
}

var _ PresencePolicy = scopePresencePolicy{}

// NewScopeIdentity builds an opaque identity from server-established axis values,
// appending only the non-empty ones (an absent axis stays absent — an empty
// string and an omitted axis read the same through Scope). The within-transport
// axis also sets the identity partition. Callers must pass only already-verified,
// server-established values, never a request-body field.
func NewScopeIdentity(transportPinned, withinTransport, subject string) AuthenticatedIdentity {
	id := AuthenticatedIdentity{Subject: subject}
	if transportPinned != "" {
		id.Scopes = append(id.Scopes, ScopeValue{Name: AxisTransportPinned, Value: transportPinned})
	}
	if withinTransport != "" {
		id.Scopes = append(id.Scopes, ScopeValue{Name: AxisWithinTransport, Value: withinTransport})
		id = id.WithPartition(withinTransport)
	}
	return id
}
