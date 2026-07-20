/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/nat"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
)

// TestPunchSignalingRoundTrip exercises the Phase 2 signaling core: an
// initiator builds a signed PunchRequest, the target validates it and
// replies with a signed PunchOffer, and the initiator validates that.
// Also covers method selection and signature tamper/wrong-key rejection.
func TestPunchSignalingRoundTrip(t *testing.T) {
	aPub, aPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bPub, bPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aID, err := aether.NewNodeID(aPub)
	if err != nil {
		t.Fatal(err)
	}
	bID, err := aether.NewNodeID(bPub)
	if err != nil {
		t.Fatal(err)
	}

	eim := nat.NATBehaviour{
		Mapping:   nat.MappingEndpointIndependent,
		Filtering: nat.FilteringEndpointIndependent,
	}
	addrsA := []net.UDPAddr{{IP: net.IPv4(1, 2, 3, 4), Port: 41641}}
	addrsB := []net.UDPAddr{{IP: net.IPv4(5, 6, 7, 8), Port: 41641}}

	a := newPunchCoordinator(aID, aPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return addrsA })
	b := newPunchCoordinator(bID, bPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return addrsB })

	// A -> B: signed request.
	req := a.buildRequest(bID)
	if req.RequesterNodeID != aID || req.TargetNodeID != bID {
		t.Fatalf("request routing wrong: %s -> %s",
			req.RequesterNodeID.Short(), req.TargetNodeID.Short())
	}
	if len(req.Signature) == 0 {
		t.Fatal("request was not signed")
	}

	// B verifies A's request and builds a signed offer.
	offer, err := b.buildOffer(req, aPub)
	if err != nil {
		t.Fatalf("buildOffer rejected a valid request: %v", err)
	}
	if offer.ResponderNodeID != bID || offer.RequesterNodeID != aID {
		t.Fatalf("offer routing wrong: responder=%s requester=%s",
			offer.ResponderNodeID.Short(), offer.RequesterNodeID.Short())
	}
	// Both peers are EIM, so ChooseMethod must pick a direct punch.
	if offer.Method != nat.PunchDirect {
		t.Fatalf("EIM/EIM should choose PunchDirect, got %v", offer.Method)
	}

	// A verifies B's offer.
	if err := a.verifyOffer(offer, bPub); err != nil {
		t.Fatalf("verifyOffer rejected a valid offer: %v", err)
	}

	// Tamper detection: a corrupted request signature must be rejected.
	forged := a.buildRequest(bID)
	forged.Signature[0] ^= 0xFF
	if _, err := b.buildOffer(forged, aPub); err == nil {
		t.Fatal("buildOffer accepted a request with a corrupted signature")
	}

	// Wrong key: verifying B's offer under A's key must fail.
	if err := a.verifyOffer(offer, aPub); err == nil {
		t.Fatal("verifyOffer accepted an offer under the wrong public key")
	}
}

// TestHolepunchRPCHandler exercises the responder side over the mesh RPC
// path: an initiator's signed PunchRequest is JSON-encoded into an
// RPCRequest payload, the handler builds a signed PunchOffer, and the
// initiator validates it. Malformed payloads must yield an unsuccessful
// response, not a transport error.
func TestHolepunchRPCHandler(t *testing.T) {
	aPub, aPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bPub, bPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aID, err := aether.NewNodeID(aPub)
	if err != nil {
		t.Fatal(err)
	}
	bID, err := aether.NewNodeID(bPub)
	if err != nil {
		t.Fatal(err)
	}

	eim := nat.NATBehaviour{
		Mapping:   nat.MappingEndpointIndependent,
		Filtering: nat.FilteringEndpointIndependent,
	}
	a := newPunchCoordinator(aID, aPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return []net.UDPAddr{{IP: net.IPv4(1, 2, 3, 4), Port: 41641}} })
	b := newPunchCoordinator(bID, bPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return []net.UDPAddr{{IP: net.IPv4(5, 6, 7, 8), Port: 41641}} })
	h := &holepunchRPCHandler{coord: b}

	if h.Name() != holepunchRPCHandlerName {
		t.Fatalf("handler name = %q, want %q", h.Name(), holepunchRPCHandlerName)
	}

	// A builds a signed request, JSON-encodes it into the RPC payload.
	req := a.buildRequest(bID)
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{ID: "rpc-1", Payload: payload})
	if err != nil {
		t.Fatalf("ExecuteRPC returned a transport error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("ExecuteRPC unsuccessful: %s", resp.Error)
	}
	if resp.ID != "rpc-1" {
		t.Fatalf("response ID = %q, want %q", resp.ID, "rpc-1")
	}

	// The payload must decode to a valid PunchOffer signed by B.
	var offer nat.PunchOffer
	if err := json.Unmarshal(resp.Payload, &offer); err != nil {
		t.Fatalf("response payload is not a PunchOffer: %v", err)
	}
	if offer.ResponderNodeID != bID || offer.RequesterNodeID != aID {
		t.Fatalf("offer routing wrong: responder=%s requester=%s",
			offer.ResponderNodeID.Short(), offer.RequesterNodeID.Short())
	}
	if offer.Method != nat.PunchDirect {
		t.Fatalf("EIM/EIM should choose PunchDirect, got %v", offer.Method)
	}
	if err := a.verifyOffer(&offer, bPub); err != nil {
		t.Fatalf("offer from RPC handler failed verification: %v", err)
	}

	// A malformed payload yields an unsuccessful response, not an error.
	bad, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{ID: "rpc-2", Payload: []byte("not json")})
	if err != nil {
		t.Fatalf("ExecuteRPC returned a transport error on bad input: %v", err)
	}
	if bad.Success {
		t.Fatal("ExecuteRPC reported success for a malformed payload")
	}
	if bad.ID != "rpc-2" {
		t.Fatalf("error response ID = %q, want %q", bad.ID, "rpc-2")
	}
}

// TestRequestOffer exercises the initiator send side: requestOffer builds
// a signed request, drives a punchRPCCaller, and decodes the offer. The
// fake caller hosts the responder-side handler so the full
// request -> RPC -> handler -> offer path is covered, plus the
// no-session and handler-rejection failure modes.
func TestRequestOffer(t *testing.T) {
	aPub, aPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bPub, bPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aID, err := aether.NewNodeID(aPub)
	if err != nil {
		t.Fatal(err)
	}
	bID, err := aether.NewNodeID(bPub)
	if err != nil {
		t.Fatal(err)
	}

	eim := nat.NATBehaviour{
		Mapping:   nat.MappingEndpointIndependent,
		Filtering: nat.FilteringEndpointIndependent,
	}
	a := newPunchCoordinator(aID, aPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return []net.UDPAddr{{IP: net.IPv4(1, 2, 3, 4), Port: 41641}} })
	b := newPunchCoordinator(bID, bPriv,
		func() nat.NATBehaviour { return eim },
		func() []net.UDPAddr { return []net.UDPAddr{{IP: net.IPv4(5, 6, 7, 8), Port: 41641}} })
	bHandler := &holepunchRPCHandler{coord: b}

	// Fake caller: routes the RPC through B's handler, as a live mesh
	// session would. Asserts the request is addressed to B.
	caller := func(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
		if nodeID != string(bID) {
			t.Fatalf("caller routed to %q, want %q", nodeID, string(bID))
		}
		if req.Handler != holepunchRPCHandlerName {
			t.Fatalf("RPC handler = %q, want %q", req.Handler, holepunchRPCHandlerName)
		}
		resp, hErr := bHandler.ExecuteRPC(ctx, &handlers.RPCRequest{ID: req.Id, Payload: req.Payload})
		if hErr != nil {
			return nil, true, hErr
		}
		return &pb.RPCResponse{Id: resp.ID, Success: resp.Success, Payload: resp.Payload, Error: resp.Error}, true, nil
	}

	offer, t0, err := a.requestOffer(context.Background(), caller, bID)
	if err != nil {
		t.Fatalf("requestOffer failed: %v", err)
	}
	if t0.IsZero() || time.Until(t0) > nat.PunchLeadTime {
		t.Fatalf("requestOffer returned implausible T0 %v (lead %v)", t0, nat.PunchLeadTime)
	}
	if offer.ResponderNodeID != bID || offer.RequesterNodeID != aID {
		t.Fatalf("offer routing wrong: responder=%s requester=%s",
			offer.ResponderNodeID.Short(), offer.RequesterNodeID.Short())
	}
	if offer.Method != nat.PunchDirect {
		t.Fatalf("EIM/EIM should choose PunchDirect, got %v", offer.Method)
	}
	if err := a.verifyOffer(offer, bPub); err != nil {
		t.Fatalf("offer from requestOffer failed verification: %v", err)
	}

	// No mesh session to the peer: requestOffer must surface an error.
	noSession := func(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
		return nil, false, nil
	}
	if _, _, err := a.requestOffer(context.Background(), noSession, bID); err == nil {
		t.Fatal("requestOffer succeeded with no mesh session")
	}

	// Handler rejection: an unsuccessful response must surface an error.
	rejecting := func(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
		return &pb.RPCResponse{Success: false, Error: "denied"}, true, nil
	}
	if _, _, err := a.requestOffer(context.Background(), rejecting, bID); err == nil {
		t.Fatal("requestOffer succeeded on a rejected request")
	}
}

// Coordinated-punch execution (ExecutePunch / ExecuteProbe) is covered by
// the transport-agnostic aether/nat package's own tests — this file
// tests only the fleet-side signaling and dial-path wiring.
