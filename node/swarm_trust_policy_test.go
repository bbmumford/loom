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

// A supplied policy that omits the self-owned prefixes still admits a node's
// own heartbeat: without them every node's PeerRecord is refused fleet-wide.
func TestASuppliedPolicyKeepsTheSelfOwnedPrefixes(t *testing.T) {
	n := newTrustNode(t)
	check := enforcingGate(t, &directory.PolicyConfig{})
	for _, topic := range directory.SelfOwnedPublishPrefixes {
		if err := check(n.record(topic)); err != nil {
			t.Errorf("a supplied policy without self-owned prefixes refused %q: %v", topic, err)
		}
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
