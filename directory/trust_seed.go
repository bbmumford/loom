/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

// SelfOwnedPublishPrefixes are the topic prefixes every key-bound node writes
// about ITSELF: its PeerRecord (FleetPeerTopic — membership, role, reach, RPC
// handler adverts) and its own latency samples (LatencyTopicPrefix). A Policy
// must allow these openly, because AuthorizePublish fails closed on any topic
// matching no rule — so a Policy that omits them rejects a node's own heartbeat,
// and every node heartbeats. Restricted families (secrets.TopicPrefix,
// "role.secrets.<role>") are deliberately absent: those require per-role
// entitlement and belong in PolicyConfig.PublishPrefixes, not here.
var SelfOwnedPublishPrefixes = []string{
	string(FleetPeerTopic),
	LatencyTopicPrefix,
}

// BaselinePolicyConfig returns a PolicyConfig pre-seeded with
// SelfOwnedPublishPrefixes as open-publish rules, so a node can always publish
// its own records. It leaves Tenants, Observers, RoleEntitlements, and the
// restricted PublishPrefixes for the caller to fill from configured
// identity/tenant/role roots — those cannot be derived from the topic namespace.
//
// This is the seed that resolves the fail-closed cutover: building a Policy from
// the ZERO PolicyConfig instead denies every publish (each topic "matches no
// publish rule, fail closed"), which is why enabling an unseeded gate rejects the
// whole fleet. Pair it with NewSwarmDirectoryObserving first to confirm the seed
// is complete against live traffic before cutting to an enforcing gate.
func BaselinePolicyConfig() PolicyConfig {
	prefixes := make([]string, len(SelfOwnedPublishPrefixes))
	copy(prefixes, SelfOwnedPublishPrefixes)
	return PolicyConfig{OpenPublishPrefixes: prefixes}
}
