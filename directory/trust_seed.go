/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"strings"

	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// FleetRouteTopic carries a node's own route path advertisements and
// withdrawals, which node/route_integration.go publishes and subscribes to.
const FleetRouteTopic = ports.Topic("fleet.route")

// SelfOwnedPublishPrefixes are the publish rules for what every key-bound node
// writes about ITSELF: its PeerRecord (FleetPeerTopic — membership, role, reach,
// RPC handler adverts), its own latency samples (LatencyTopicPrefix), its mesh
// database cluster metadata (MeshDBMemberTopic) and its route advertisements
// (FleetRouteTopic). LatencyTopicPrefix ends in "." and covers the per-node
// topics LatencyTopic builds; each of the other three names one topic and
// covers no topic that only starts with it (Policy.AuthorizePublish). A Policy
// must allow these openly, because AuthorizePublish fails closed on any topic
// matching no rule — so a Policy that omits them rejects a node's own
// heartbeat, and every node heartbeats. A Policy that opens one admits its
// topics before it reads any restricted rule, so a PolicyConfig.PublishPrefixes
// rule above or on a self-owned topic does not close it. The role topics
// (secrets.ClaimTopicPrefix and secrets.TopicPrefix, "role.claim.<role>" and
// "role.secrets.<role>") are deliberately absent: only a role's entitled nodes
// may write them, so WithRoleEntitlements puts them in
// PolicyConfig.PublishPrefixes.
var SelfOwnedPublishPrefixes = []string{
	string(FleetPeerTopic),
	LatencyTopicPrefix,
	string(MeshDBMemberTopic),
	string(FleetRouteTopic),
}

// BaselinePolicyConfig returns a PolicyConfig pre-seeded with
// SelfOwnedPublishPrefixes as open-publish rules, so a node can always publish
// its own records. It leaves Partitions, Observers, RoleEntitlements, and the
// restricted PublishPrefixes for the caller to fill from configured
// identity/partition/role roots — those cannot be derived from the topic namespace.
//
// This seed is what makes the fail-closed gate safe to enforce: building a Policy from
// the ZERO PolicyConfig instead denies every publish (each topic "matches no
// publish rule, fail closed"), which is why enabling an unseeded gate rejects the
// whole fleet. Pair it with NewSwarmDirectoryObserving first to confirm the seed
// is complete against live traffic before cutting to an enforcing gate.
func BaselinePolicyConfig() PolicyConfig {
	prefixes := make([]string, len(SelfOwnedPublishPrefixes))
	copy(prefixes, SelfOwnedPublishPrefixes)
	return PolicyConfig{OpenPublishPrefixes: prefixes}
}

// WithRoleEntitlements returns a copy of cfg in which each role's entitled
// nodes hold that role (RoleEntitlements) and are the only writers of its two
// role topics: secrets.ClaimTopic(role), the takeover claim role takeover
// publishes, and secrets.Topic(role), the sealed config bundle the role's
// holder republishes. A role topic of a role absent from entitlements matches
// no rule, so the fail-closed AuthorizePublish refuses it. cfg is not changed.
//
// Each rule is the role topic's full name, which Policy.AuthorizePublish
// matches as that one topic, so the rule for role R covers no topic of a role
// whose name starts with R. A role whose name is empty or ends in "." gets no
// entitlement and no rule: its topic names end in ".", which AuthorizePublish
// reads as a namespace rule covering the topics of every role named beneath it.
func WithRoleEntitlements(cfg PolicyConfig, entitlements map[string][]ports.NodeID) PolicyConfig {
	out := cfg.Clone()
	if len(entitlements) == 0 {
		return out
	}
	if out.RoleEntitlements == nil {
		out.RoleEntitlements = map[string][]ports.NodeID{}
	}
	if out.PublishPrefixes == nil {
		out.PublishPrefixes = map[string][]ports.NodeID{}
	}
	for role, nodes := range entitlements {
		if role == "" || strings.HasSuffix(role, ".") {
			continue
		}
		out.RoleEntitlements[role] = append(out.RoleEntitlements[role], nodes...)
		for _, topic := range []ports.Topic{secrets.ClaimTopic(role), secrets.Topic(role)} {
			out.PublishPrefixes[string(topic)] = append(out.PublishPrefixes[string(topic)], nodes...)
		}
	}
	return out
}

// Clone returns a PolicyConfig that shares no slice or map with c, so a
// caller can extend the copy (node's trust shadow adds partitions) without
// changing the config it was given.
func (c PolicyConfig) Clone() PolicyConfig {
	out := PolicyConfig{
		Partitions:          append([]string(nil), c.Partitions...),
		Observers:           append([]ports.NodeID(nil), c.Observers...),
		OpenPublishPrefixes: append([]string(nil), c.OpenPublishPrefixes...),
	}
	if c.RoleEntitlements != nil {
		out.RoleEntitlements = make(map[string][]ports.NodeID, len(c.RoleEntitlements))
		for k, v := range c.RoleEntitlements {
			out.RoleEntitlements[k] = append([]ports.NodeID(nil), v...)
		}
	}
	if c.PublishPrefixes != nil {
		out.PublishPrefixes = make(map[string][]ports.NodeID, len(c.PublishPrefixes))
		for k, v := range c.PublishPrefixes {
			out.PublishPrefixes[k] = append([]ports.NodeID(nil), v...)
		}
	}
	return out
}
