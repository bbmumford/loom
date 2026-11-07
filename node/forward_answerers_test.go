/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/swarm"
)

// These tests hold the forwarder to the operation's answerers: a relay sends a
// request only to a node whose record lists its handler, as the origin's
// findNodeForRole picks, and refuses typed before sending when there is none.
// holder-1 claims the role orbtr.audit and lists only orbtr.audit.Query, so it
// answers orbtr.audit.Write as not found at its RPCServer.

func roleClaimantTable(t *testing.T) *RoleTable {
	t.Helper()
	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	if err := table.onRecord(swarm.Record{NodeID: swarm.NodeID("holder-1"),
		Body: publishedAdvert(t, capabilityOp("Query", "orbtr:audit.read"))}); err != nil {
		t.Fatal(err)
	}
	if n := len(table.HandlerAdverts(advertFQN)); n != 0 {
		t.Fatalf("HandlersByName(%s) lists %d nodes, want 0", advertFQN, n)
	}
	return table
}

// A forward of an operation no peer advertises is refused before send with the
// typed refusal naming the operation, and the role claimant receives nothing.
func TestAForwardOfAnOperationNoPeerAdvertisesIsRefusedBeforeSending(t *testing.T) {
	hop := &hopSession{remote: "holder-1", proto: aether.ProtoNoise}
	f := forwarderWith(hop)
	f.rt.swarm = &SwarmIntegration{RoleTable: roleClaimantTable(t)}

	resp, err := f.Forward(context.Background(), &pb.RPCRequest{Id: "r", Handler: advertFQN, Hops: 1, Context: map[string]string{}})
	if hop.sends() != 0 {
		t.Fatalf("the forward of %s was sent to holder-1, which claims the role and does not advertise the operation", advertFQN)
	}
	if resp != nil || !errors.Is(err, rpc.ErrNoAnswerer) {
		t.Fatalf("Forward = (%v, %v), want the typed no-answerer refusal", resp, err)
	}
	var typed *dispatch.NoAnswererError
	if !errors.As(err, &typed) || typed.Handler != advertFQN || !strings.Contains(err.Error(), advertFQN) {
		t.Fatalf("Forward err = %v, want a *dispatch.NoAnswererError naming %s", err, advertFQN)
	}
	if !dispatch.Unsent(err) {
		t.Fatalf("Unsent(%v) = false, want true: nothing left this node", err)
	}
}

// A relay for a request naming a target it has no path to does not hand the
// request to a node of the role that does not advertise the operation.
func TestARelayNeverHandsATargetedCallToARoleClaimantThatDoesNotAdvertiseIt(t *testing.T) {
	hop := &hopSession{remote: "holder-1", proto: aether.ProtoNoise}
	f := forwarderWith(hop)
	f.rt.swarm = &SwarmIntegration{RoleTable: roleClaimantTable(t)}

	_, err := f.Forward(context.Background(), &pb.RPCRequest{Id: "r", Handler: advertFQN, TargetNodeId: "target-x", Hops: 1, Context: map[string]string{}})
	if hop.sends() != 0 {
		t.Fatalf("a call naming target-x was sent to holder-1, which does not advertise %s", advertFQN)
	}
	if err == nil || !dispatch.Unsent(err) {
		t.Fatalf("Forward err = %v, want a refusal before send", err)
	}
}

// The relay's refusal is stamped on the failed response RPCServer returns, so
// the origin rebuilds it typed instead of reading a handler rejection.
func TestTheNoAnswererRefusalIsStampedOnTheRelaysResponse(t *testing.T) {
	f := forwarderWith()
	f.rt.swarm = &SwarmIntegration{RoleTable: roleClaimantTable(t)}
	req := &pb.RPCRequest{Id: "r", Handler: advertFQN, Hops: 1, Context: map[string]string{}}

	_, err := f.Forward(context.Background(), req)
	if !errors.Is(err, rpc.ErrNoAnswerer) {
		t.Fatalf("Forward err = %v, want the typed no-answerer refusal", err)
	}
	failed := forwardFailure(req, err, time.Now(), err)
	if failed.Metadata["refusal"] == "" {
		t.Fatalf("the refusal %v reaches the origin unstamped (metadata %v)", err, failed.Metadata)
	}
}
