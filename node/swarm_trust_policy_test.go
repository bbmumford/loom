/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"crypto/ed25519"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// These tests drive the enforcing trust gate a Runtime builds from its Config
// (Runtime.swarmTrustGate, which InitSwarm wires as swarm's TrustCheck) over
// the topics loom's own writers publish beyond a node's heartbeat: fleet.route
// from the route publisher, and role.claim.<role> and role.secrets.<role>
// from role takeover and the role secrets publisher.

type trustNode struct {
	id  ports.NodeID
	pub ed25519.PublicKey
}

func newTrustNode(t *testing.T) trustNode {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return trustNode{id: ports.NodeID(string(id)), pub: pub}
}

func (n trustNode) record(topic string) swarm.Record {
	return swarm.Record{Topic: swarm.Topic(topic), NodeID: swarm.NodeID(string(n.id)), PubKey: n.pub}
}

func enforcingGate(t *testing.T, policy *directory.PolicyConfig) func(swarm.Record) error {
	t.Helper()
	rt := &Runtime{cfg: Config{SwarmTrustMode: "enforce", SwarmTrustPolicy: policy}}
	check, gate := rt.swarmTrustGate()
	if check == nil || gate == nil {
		t.Fatal("SwarmTrustMode enforce built no gate")
	}
	return check
}

// A peer's route advertisement passes the enforcing gate with no policy
// supplied: fleet.route is a topic every node writes about itself, like its
// PeerRecord, so the baseline admits it.
func TestEnforceAdmitsAPeersRouteAdvertisement(t *testing.T) {
	peer := newTrustNode(t)
	check := enforcingGate(t, nil)
	if err := check(peer.record(string(fleetRouteTopic))); err != nil {
		t.Fatalf("the enforcing gate refused a peer's %s record: %v", fleetRouteTopic, err)
	}
}

// With the role entitlements supplied, an entitled node's takeover claim and
// sealed-bundle republish pass; an unentitled node's are refused, as is a
// claim on a role nobody is entitled to and a topic matching no rule.
func TestEnforceAdmitsEntitledRoleTopicsAndRefusesTheRest(t *testing.T) {
	holder := newTrustNode(t)
	outsider := newTrustNode(t)
	const role = "orbtr.auth"
	policy := directory.WithRoleEntitlements(directory.BaselinePolicyConfig(),
		map[string][]ports.NodeID{role: {holder.id}})
	check := enforcingGate(t, &policy)

	for _, tc := range []struct {
		name   string
		rec    swarm.Record
		accept bool
	}{
		{"an entitled node's claim", holder.record(string(secrets.ClaimTopic(role))), true},
		{"an entitled node's sealed bundle", holder.record(string(secrets.Topic(role))), true},
		{"an unentitled node's claim", outsider.record(string(secrets.ClaimTopic(role))), false},
		{"an unentitled node's sealed bundle", outsider.record(string(secrets.Topic(role))), false},
		{"a claim on a role nobody holds", holder.record(string(secrets.ClaimTopic("orbtr.billing"))), false},
		{"a topic matching no rule", holder.record("no.rule.here"), false},
		{"the entitled node's own heartbeat", holder.record(string(directory.FleetPeerTopic)), true},
	} {
		err := check(tc.rec)
		if tc.accept && err != nil {
			t.Errorf("%s (%s) was refused: %v", tc.name, tc.rec.Topic, err)
		}
		if !tc.accept && err == nil {
			t.Errorf("%s (%s) was admitted", tc.name, tc.rec.Topic)
		}
	}
}

// A role's entitlement covers that role's two topics and no other role's. The
// holder of one role is refused the claim and the sealed bundle of every role
// whose name only starts with its own, and of two roles named that way each
// admits its own entitled node alone.
func TestARoleEntitlementCoversOnlyItsOwnRoleTopics(t *testing.T) {
	holder := newTrustNode(t)
	admin := newTrustNode(t)
	const role, longer = "orbtr.auth", "orbtr.auth.admin"
	policy := directory.WithRoleEntitlements(directory.BaselinePolicyConfig(),
		map[string][]ports.NodeID{role: {holder.id}, longer: {admin.id}})
	check := enforcingGate(t, &policy)

	for _, tc := range []struct {
		name   string
		node   trustNode
		role   string
		accept bool
	}{
		{"the holder, on its role", holder, role, true},
		{"the longer role's node, on the longer role", admin, longer, true},
		{"the holder, on a role one letter longer", holder, "orbtr.authz", false},
		{"the holder, on a role several letters longer", holder, "orbtr.authority", false},
		{"the holder, on an unheld role beneath its own", holder, "orbtr.auth.other", false},
		{"the holder, on the longer role", holder, longer, false},
		{"the longer role's node, on the shorter role", admin, role, false},
		{"the longer role's node, on a role beneath its own", admin, "orbtr.auth.admin.x", false},
	} {
		for _, topic := range []ports.Topic{secrets.ClaimTopic(tc.role), secrets.Topic(tc.role)} {
			err := check(tc.node.record(string(topic)))
			if tc.accept && err != nil {
				t.Errorf("%s (%s) was refused: %v", tc.name, topic, err)
			}
			if !tc.accept && err == nil {
				t.Errorf("%s (%s) was admitted", tc.name, topic)
			}
		}
	}
}

// The baseline admits the topics a node writes about itself and no topic whose
// name only starts with one of them: fleet.route, fleet.peer and meshdb.member
// are one topic each, and fleet.latency.<node> is the one family.
func TestEnforceRefusesATopicThatOnlyStartsWithASelfOwnedOne(t *testing.T) {
	n := newTrustNode(t)
	check := enforcingGate(t, nil)
	for _, topic := range []string{
		string(directory.FleetRouteTopic),
		string(directory.FleetPeerTopic),
		string(directory.MeshDBMemberTopic),
		string(directory.LatencyTopic(n.id)),
	} {
		if err := check(n.record(topic)); err != nil {
			t.Errorf("the baseline refused a node's own %q record: %v", topic, err)
		}
	}
	for _, topic := range []string{
		"fleet.routeadmin.x", "fleet.routes", "fleet.route.x",
		"fleet.peerage", "fleet.peer.x",
		"meshdb.memberx", "meshdb.member.x",
	} {
		if err := check(n.record(topic)); err == nil {
			t.Errorf("the baseline admitted %q, which no rule names", topic)
		}
	}
}

// Whatever policy is supplied, the gate admits a node's own records on every
// self-owned topic: a policy that omits the self-owned prefixes, and one whose
// restricted rules cover or name them. Without that every node's PeerRecord is
// refused fleet-wide. A supplied restricted rule still decides the topics
// beside the self-owned ones.
func TestASuppliedPolicyKeepsTheSelfOwnedPrefixes(t *testing.T) {
	n := newTrustNode(t)
	admin := newTrustNode(t)
	check := enforcingGate(t, &directory.PolicyConfig{})
	for _, topic := range directory.SelfOwnedPublishPrefixes {
		if err := check(n.record(topic)); err != nil {
			t.Errorf("a supplied policy without self-owned prefixes refused %q: %v", topic, err)
		}
	}

	ownLatency := string(directory.LatencyTopic(n.id))
	selfOwned := []string{
		string(directory.FleetPeerTopic),
		string(directory.FleetRouteTopic),
		string(directory.MeshDBMemberTopic),
		ownLatency,
	}
	writers := []ports.NodeID{admin.id}
	for _, tc := range []struct {
		name       string
		restricted map[string][]ports.NodeID
	}{
		{"a restricted namespace above them", map[string][]ports.NodeID{"fleet.": writers, "meshdb.": writers}},
		{"a restricted default rule", map[string][]ports.NodeID{"": writers}},
		{"restricted rules naming them", map[string][]ports.NodeID{
			string(directory.FleetPeerTopic):    writers,
			string(directory.FleetRouteTopic):   writers,
			string(directory.MeshDBMemberTopic): writers,
			directory.LatencyTopicPrefix:        writers,
			ownLatency:                          writers,
		}},
	} {
		check := enforcingGate(t, &directory.PolicyConfig{PublishPrefixes: tc.restricted})
		for _, topic := range selfOwned {
			if err := check(n.record(topic)); err != nil {
				t.Errorf("a supplied policy with %s refused a node's own %q record: %v", tc.name, topic, err)
			}
		}
	}

	check = enforcingGate(t, &directory.PolicyConfig{PublishPrefixes: map[string][]ports.NodeID{"fleet.": writers}})
	if err := check(n.record("fleet.admin")); err == nil {
		t.Error("a node outside the supplied fleet. rule was admitted on fleet.admin")
	}
	if err := check(admin.record("fleet.admin")); err != nil {
		t.Errorf("the supplied fleet. rule's writer was refused on fleet.admin: %v", err)
	}
}

// WithRoleEntitlements returns a copy: the config it was given, maps and
// slices included, is unchanged. swarmTrustPolicy relies on the same copy so
// the trust shadow's partitions never reach the composition root's config.
func TestWithRoleEntitlementsLeavesItsInputUnchanged(t *testing.T) {
	base := directory.BaselinePolicyConfig()
	base.RoleEntitlements = map[string][]ports.NodeID{"r": {"vl1_a"}}
	base.PublishPrefixes = map[string][]ports.NodeID{"x.": {"vl1_a"}}
	open := len(base.OpenPublishPrefixes)

	out := directory.WithRoleEntitlements(base, map[string][]ports.NodeID{"r": {"vl1_b"}})
	if len(out.RoleEntitlements["r"]) != 2 {
		t.Fatalf("premise wrong: the copy holds %v for role r, want both nodes", out.RoleEntitlements["r"])
	}
	if len(base.RoleEntitlements["r"]) != 1 || len(base.PublishPrefixes) != 1 ||
		len(base.OpenPublishPrefixes) != open {
		t.Fatalf("WithRoleEntitlements changed the config it was given: %+v", base)
	}

	supplied := directory.PolicyConfig{Partitions: []string{"p1"}}
	got := swarmTrustPolicy(&supplied)
	got.Partitions = append(got.Partitions, "p2")
	got.OpenPublishPrefixes[0] = "changed"
	if len(supplied.Partitions) != 1 || len(supplied.OpenPublishPrefixes) != 0 {
		t.Fatalf("swarmTrustPolicy's result shares storage with the supplied config: %+v", supplied)
	}
}
