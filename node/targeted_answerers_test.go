/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// These tests hold a call that names its target node to the operation's
// answerers, as findNodeForRole holds a routed call: HWPCaller.CallNode and
// OpenNodeStream ask ConnectionManager.CheckAnswerers before anything leaves, and
// a named node whose record does not list the operation is refused there with a
// *dispatch.NoAnswererError naming the operation and the node. The named node is
// judged by the operation its record lists, whatever role it claims and whatever
// role the call names, because those records are the ones
// LADDirectory.HandlersByName answers from (RoleTable.HandlerAdverts). Sent, the
// call's request names no target node (CallNode leaves TargetNodeId empty), so
// RPCServer.handleRequest on a named node that lacks the operation would forward
// it to another node that lists it, and a stream open would be refused by that
// node's HandlerRegistry.DispatchStream after the open, untyped.

const advertNodeD = "dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44"

// wantTargetedNoAnswerer fails t unless err is the typed refusal naming handler
// and node, as a route refusal nothing was sent on.
func wantTargetedNoAnswerer(t *testing.T, err error, handler, node string) {
	t.Helper()
	if !errors.Is(err, rpc.ErrNoAnswerer) {
		t.Fatalf("err = %v, want the typed no-answerer refusal", err)
	}
	var typed *dispatch.NoAnswererError
	if !errors.As(err, &typed) || typed.Handler != handler || typed.Node != node {
		t.Fatalf("err = %v (%+v), want a *dispatch.NoAnswererError naming %s and node %s", err, typed, handler, node)
	}
	if !strings.Contains(err.Error(), handler) || !strings.Contains(err.Error(), node) {
		t.Fatalf("err text %q does not name %s and %s", err.Error(), handler, node)
	}
	if !dispatch.Unsent(err) {
		t.Fatalf("Unsent(%v) = false, want true: nothing left this node", err)
	}
}

// The origin's check refuses a named node absent from the operation's adverts:
// a role claimant that lists other operations, the same claimant beside a node
// that advertises the operation, and a node no record names at all. A named node
// that advertises the operation is let through.
func TestACallNamingANodeThatDoesNotAdvertiseTheOperationIsRefusedNamingBoth(t *testing.T) {
	alone := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Query", "orbtr:audit.read")),
	})
	wantTargetedNoAnswerer(t, alone.CheckAnswerers(advertRole, advertFQN, testNodeIDB), advertFQN, testNodeIDB)

	beside := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Query", "orbtr:audit.read")),
		advertNodeC: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
	})
	wantTargetedNoAnswerer(t, beside.CheckAnswerers(advertRole, advertFQN, testNodeIDB), advertFQN, testNodeIDB)
	wantTargetedNoAnswerer(t, beside.CheckAnswerers(advertRole, advertFQN, advertNodeD), advertFQN, advertNodeD)
	if err := beside.CheckAnswerers(advertRole, advertFQN, advertNodeC); err != nil {
		t.Fatalf("CheckAnswerers naming the advertiser = %v, want nil", err)
	}
	if err := beside.CheckAnswerers(advertRole, advertFQN, ""); err != nil {
		t.Fatalf("routed CheckAnswerers = %v, want nil: the routed call has an answerer", err)
	}
}

// HWPCaller.CallNode and OpenNodeStream return the refusal before they ask the
// finder for the named node's session.
func TestATargetedCallAndStreamToANonAdvertiserAreRefusedBeforeTheFinderIsAsked(t *testing.T) {
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Query", "orbtr:audit.read")),
	})
	bind(m, testNodeIDB, noiseSession())
	caller := dispatch.NewHWPCaller(m)
	defer caller.Close()

	_, err := caller.CallNode(context.Background(), testNodeIDB, advertRole, advertFQN, []byte("x"))
	wantTargetedNoAnswerer(t, err, advertFQN, testNodeIDB)
	_, err = caller.OpenNodeStream(context.Background(), testNodeIDB, advertRole, advertFQN)
	wantTargetedNoAnswerer(t, err, advertFQN, testNodeIDB)
}

// Over real sessions: a joiner's call naming the seed for an operation the seed
// does not advertise is refused at the joiner, typed, and never answered by the
// seed as a handler rejection; a call naming the seed for its own operation is
// answered, so the session carries targeted calls.
func TestACallNamingANodeThatDoesNotAdvertiseTheOperationIsNotSentOverRealSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("starts two mesh runtimes")
	}
	seed := relayTestRuntime(t, "seed", nil)
	host, roots := seedJoinTestServer(t, seed)
	publishRelayTestOp(t, seed, "AtSeed")
	joiner := relayTestRuntime(t, "joiner", nil)
	publishRelayTestOp(t, joiner, "AtJoiner")
	joinRelayTestSeed(t, joiner, host, roots)
	waitRelayTest(t, 60*time.Second, "the joiner to hold the seed's advert", func() bool {
		return holdsRelayTestAdvert(joiner, "AtSeed", seed)
	})

	caller := dispatch.NewHWPCaller(joiner.ConnManager())
	t.Cleanup(caller.Close)
	payload, err := proto.Marshal(wrapperspb.String("who"))
	if err != nil {
		t.Fatal(err)
	}
	seedID := string(seed.NodeID())

	var answered error
	for attempt := 0; attempt < 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := caller.CallNode(ctx, seedID, "orbtr.relaytest", relayTestFQN("AtSeed"), payload)
		cancel()
		if answered = err; err == nil {
			var got wrapperspb.StringValue
			if err := proto.Unmarshal(out, &got); err != nil || got.GetValue() != "AtSeed" {
				t.Fatalf("the seed answered %q (%v), want AtSeed", got.GetValue(), err)
			}
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if answered != nil {
		t.Fatalf("a call naming the seed for its own operation was not answered: %v", answered)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := caller.CallNode(ctx, seedID, "orbtr.relaytest", relayTestFQN("AtJoiner"), payload)
	if err == nil {
		var got wrapperspb.StringValue
		_ = proto.Unmarshal(out, &got)
		t.Fatalf("the call naming the seed for an operation it does not advertise was sent and answered %q", got.GetValue())
	}
	if strings.Contains(err.Error(), "not found on target node") || errors.Is(err, dispatch.ErrHandlerError) {
		t.Fatalf("the call was sent and the seed rejected it as a handler that is not there: %v", err)
	}
	wantTargetedNoAnswerer(t, err, relayTestFQN("AtJoiner"), seedID)
}

// advertClaimingNoRole is publishedAdvert's record with no role claimed: a node
// whose record lists the operations ops name and sits in no role's index.
func advertClaimingNoRole(t *testing.T, ops ...advertOp) []byte {
	t.Helper()
	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(publishedAdvert(t, ops...), &rec); err != nil {
		t.Fatal(err)
	}
	rec.Capabilities.Roles = nil
	body, err := proto.Marshal(&rec)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// A named node is judged by its record's operations, not by the role the call
// names: a node listing the operation is let through under its own role, another
// role, no role, and when its record claims no role at all; a node that does not
// list the operation is refused under every role, as before.
func TestATargetedCallIsJudgedByTheOperationTheNamedNodeListsWhateverRoleItNames(t *testing.T) {
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		advertNodeC: advertClaimingNoRole(t, capabilityOp("Write", "orbtr:audit.write")),
	})
	for _, role := range []string{advertRole, "orbtr.otherrole", ""} {
		for _, node := range []string{testNodeIDB, advertNodeC} {
			if err := m.CheckAnswerers(role, advertFQN, node); err != nil {
				t.Fatalf("CheckAnswerers(%q, %s, %s) = %v, want nil: the node's record lists the operation",
					role, advertFQN, truncID(node), err)
			}
		}
		wantTargetedNoAnswerer(t, m.CheckAnswerers(role, "orbtr.audit.Query", testNodeIDB), "orbtr.audit.Query", testNodeIDB)
		wantTargetedNoAnswerer(t, m.CheckAnswerers(role, advertFQN, advertNodeD), advertFQN, advertNodeD)
	}
}

// The refusals that come first are judged over the same records: under a role
// the adverts do not claim, a call naming either of two nodes whose adverts name
// different requirements, or naming a node whose advert names none beside one
// that does, is refused as a disagreement, never let through or refused as a
// missing operation.
func TestATargetedCallUnderAnotherRoleIsRefusedWhenTheOperationsAdvertsDisagree(t *testing.T) {
	const otherRole = "orbtr.otherrole"
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		advertNodeC: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.admin")),
	})
	for _, node := range []string{testNodeIDB, advertNodeC} {
		if err := m.CheckAnswerers(otherRole, advertFQN, node); !errors.Is(err, rpc.ErrAnswerersDisagree) {
			t.Fatalf("CheckAnswerers(%q, %s, %s) = %v, want the disagreement refusal", otherRole, advertFQN, truncID(node), err)
		}
	}

	bare, err := proto.Marshal(&swarmpb.PeerRecord{RpcHandlers: []string{advertFQN}})
	if err != nil {
		t.Fatal(err)
	}
	m = advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		advertNodeC: bare,
	})
	if err := m.CheckAnswerers(otherRole, advertFQN, advertNodeC); !errors.Is(err, rpc.ErrAnswerersDisagree) {
		t.Fatalf("CheckAnswerers naming the bare peer = %v, want the disagreement refusal", err)
	}
	if err := m.CheckAnswerers(otherRole, advertFQN, testNodeIDB); err != nil {
		t.Fatalf("CheckAnswerers naming the holder = %v, want nil", err)
	}
}

// Over real sessions: a joiner's call naming the seed for an operation the seed
// advertises is answered by the seed under a role string the seed's record does
// not claim, and a call naming the seed for an operation only the joiner
// advertises is still refused at the joiner under that role, typed.
func TestACallNamingTheSeedForItsOwnOperationIsAnsweredUnderARoleItDoesNotClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("starts two mesh runtimes")
	}
	const otherRole = "orbtr.otherrole"
	seed := relayTestRuntime(t, "seed", nil)
	host, roots := seedJoinTestServer(t, seed)
	publishRelayTestOp(t, seed, "AtSeed")
	joiner := relayTestRuntime(t, "joiner", nil)
	publishRelayTestOp(t, joiner, "AtJoiner")
	joinRelayTestSeed(t, joiner, host, roots)
	waitRelayTest(t, 60*time.Second, "the joiner to hold the seed's advert", func() bool {
		return holdsRelayTestAdvert(joiner, "AtSeed", seed)
	})
	if slices.Contains(joiner.swarm.RoleTable.RolesOf(string(seed.NodeID())), otherRole) {
		t.Fatalf("the seed's record claims %s; the test needs a role it does not claim", otherRole)
	}

	caller := dispatch.NewHWPCaller(joiner.ConnManager())
	t.Cleanup(caller.Close)
	payload, err := proto.Marshal(wrapperspb.String("who"))
	if err != nil {
		t.Fatal(err)
	}
	seedID := string(seed.NodeID())

	var answered error
	for attempt := 0; attempt < 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := caller.CallNode(ctx, seedID, otherRole, relayTestFQN("AtSeed"), payload)
		cancel()
		if answered = err; err == nil {
			var got wrapperspb.StringValue
			if err := proto.Unmarshal(out, &got); err != nil || got.GetValue() != "AtSeed" {
				t.Fatalf("the seed answered %q (%v), want AtSeed", got.GetValue(), err)
			}
			break
		}
		if errors.Is(err, rpc.ErrNoAnswerer) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if answered != nil {
		t.Fatalf("a call naming the seed for its own operation under role %s was not answered: %v", otherRole, answered)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = caller.CallNode(ctx, seedID, otherRole, relayTestFQN("AtJoiner"), payload)
	wantTargetedNoAnswerer(t, err, relayTestFQN("AtJoiner"), seedID)
}
