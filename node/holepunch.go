/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Coordinated NAT hole-punch signaling for fleet NAT traversal.
//
// punchCoordinator is the signaling core: an initiator builds a signed
// PunchRequest, the target validates it and replies with a signed
// PunchOffer carrying the method aether/nat.ChooseMethod selects from
// both sides' RFC 5780 NAT class. The synchronized simultaneous-dial
// execution (Task 2.2) consumes the verified offers produced here.
//
// holepunchRPCHandler exposes the responder side over the mesh RPC
// path: a peer invokes the "holepunch" handler over its existing
// authenticated session, the handler builds and returns a PunchOffer.
// The coordinator owns no transport, so its crypto + method-selection
// logic stays unit-testable in isolation from the RPC subsystem.
package node

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/nat"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/trace"
)

// holepunchRPCHandlerName is the mesh RPC handler name peers invoke to
// exchange punch payloads.
const holepunchRPCHandlerName = "holepunch"

// punchCoordinator builds, signs and verifies coordinated hole-punch
// payloads for one mesh node.
type punchCoordinator struct {
	selfID     aether.NodeID
	priv       ed25519.PrivateKey
	localNAT   func() nat.NATBehaviour // local RFC 5780 class (Runtime.NATBehaviour)
	localAddrs func() []net.UDPAddr    // local candidate UDP addresses
}

// newPunchCoordinator builds a coordinator for a node. localNAT and
// localAddrs are snapshot functions — called fresh on each build so the
// payload always reflects current classification + addressing.
func newPunchCoordinator(
	selfID aether.NodeID,
	priv ed25519.PrivateKey,
	localNAT func() nat.NATBehaviour,
	localAddrs func() []net.UDPAddr,
) *punchCoordinator {
	return &punchCoordinator{
		selfID:     selfID,
		priv:       priv,
		localNAT:   localNAT,
		localAddrs: localAddrs,
	}
}

// buildRequest constructs and signs a PunchRequest addressed to target.
// The signature (Ed25519 over the canonical SignBytes) lets the target
// confirm origin even though the carrier session is already
// authenticated — defence in depth, and a prerequisite for any future
// relayed signaling.
func (pc *punchCoordinator) buildRequest(target aether.NodeID) *nat.PunchRequest {
	req := &nat.PunchRequest{
		RequesterNodeID: pc.selfID,
		TargetNodeID:    target,
		LocalAddrs:      pc.localAddrs(),
		Behaviour:       pc.localNAT(),
		Timestamp:       time.Now().UnixNano(),
	}
	req.Sign(pc.priv)
	return req
}

// buildOffer validates an inbound PunchRequest — its Ed25519 signature
// is checked against requesterPub — and, when valid, constructs and
// signs a PunchOffer. A signature failure returns the error and no
// offer. Used on the standalone-verification path (e.g. tests, or any
// future relayed signaling where the carrier is not authenticated).
func (pc *punchCoordinator) buildOffer(req *nat.PunchRequest, requesterPub ed25519.PublicKey) (*nat.PunchOffer, error) {
	if err := req.Verify(requesterPub); err != nil {
		return nil, err
	}
	return pc.buildOfferAuthenticated(req), nil
}

// buildOfferAuthenticated constructs and signs a PunchOffer in reply to
// an inbound request WITHOUT a standalone signature check. Use only
// when the request arrived over a channel that has already
// authenticated the peer — the mesh RPC path rides an authenticated
// session, so the session handshake is the security boundary and the
// payload signature would be redundant. A forged RequesterNodeID is not
// an escalation: the offer is simply addressed to the wrong node and
// the spoofer's own punch fails.
//
// The offer's Method is chosen by aether/nat.ChooseMethod from both
// sides' NAT behaviour: PunchDirect when both ends have endpoint-
// independent mapping, PunchPortPrediction otherwise.
func (pc *punchCoordinator) buildOfferAuthenticated(req *nat.PunchRequest) *nat.PunchOffer {
	local := pc.localNAT()
	offer := &nat.PunchOffer{
		ResponderNodeID: pc.selfID,
		RequesterNodeID: req.RequesterNodeID,
		LocalAddrs:      pc.localAddrs(),
		Behaviour:       local,
		Method:          nat.ChooseMethod(local, req.Behaviour),
		Timestamp:       time.Now().UnixNano(),
	}
	offer.Sign(pc.priv)
	return offer
}

// verifyOffer checks an inbound PunchOffer's Ed25519 signature against
// the responder's public key. A nil return means the offer is
// authentic and safe for the punch executor (Task 2.2) to act on.
func (pc *punchCoordinator) verifyOffer(offer *nat.PunchOffer, responderPub ed25519.PublicKey) error {
	return offer.Verify(responderPub)
}

// punchRPCCounter sources unique RPC request IDs for punch signaling.
var punchRPCCounter uint64

// punchRPCCaller invokes a mesh RPC handler on a specific peer over that
// peer's existing authenticated session. ConnectionManager.CallViaBidi
// satisfies it: (response, sessionFound, error). A false sessionFound
// means no BidiRPC channel exists for the peer.
type punchRPCCaller func(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error)

// requestOffer is the initiator side of punch signaling. It builds a
// signed PunchRequest for target, invokes the peer's "holepunch" RPC
// handler over target's existing authenticated mesh session, decodes the
// returned PunchOffer, and derives the synchronized fire time T0.
//
// T0 = nat.PunchFireTime(request). Both peers compute it from the
// request — the initiator from the request it built, the responder from
// the request it decoded in ExecuteRPC — so neither needs a clock-synced
// handshake to agree on when to dial.
//
// The offer is NOT standalone signature-verified here: the carrier
// session already authenticated the responder, mirroring the responder
// side's buildOfferAuthenticated. The offer's Method (chosen by the peer
// via aether/nat.ChooseMethod) and candidate addresses are what
// nat.ExecutePunch consumes.
func (pc *punchCoordinator) requestOffer(ctx context.Context, call punchRPCCaller, target aether.NodeID) (*nat.PunchOffer, time.Time, error) {
	req := pc.buildRequest(target)
	t0 := nat.PunchFireTime(req)
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("holepunch: encode request: %w", err)
	}
	rpcReq := &pb.RPCRequest{
		Id:           fmt.Sprintf("holepunch-%d", atomic.AddUint64(&punchRPCCounter, 1)),
		Handler:      holepunchRPCHandlerName,
		Payload:      payload,
		TargetNodeId: string(target),
		TraceId:      trace.FromContext(ctx),
	}
	resp, found, err := call(ctx, string(target), rpcReq)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("holepunch: RPC to %s: %w", target.Short(), err)
	}
	if !found {
		return nil, time.Time{}, fmt.Errorf("holepunch: no mesh session to %s", target.Short())
	}
	if resp == nil || !resp.Success {
		errMsg := "no response"
		if resp != nil {
			errMsg = resp.Error
		}
		return nil, time.Time{}, fmt.Errorf("holepunch: %s rejected request: %s", target.Short(), errMsg)
	}
	var offer nat.PunchOffer
	if err := json.Unmarshal(resp.Payload, &offer); err != nil {
		return nil, time.Time{}, fmt.Errorf("holepunch: decode offer from %s: %w", target.Short(), err)
	}
	return &offer, t0, nil
}

// (Coordinated-punch execution — ExecutePunch / ExecuteProbe / fire-time
// derivation — lives in the transport-agnostic aether/nat package so the
// HSTLES fleet and the ORBTR agent drive identical punch logic. This
// file holds only the fleet-side wiring: signaling over mesh RPC and
// the dial-path integration.)

// holepunchRPCHandler is the responder side of punch signaling. It is
// registered into the mesh RPC HandlerRegistry under
// holepunchRPCHandlerName; a peer invokes it over its existing
// authenticated mesh session with a JSON-encoded nat.PunchRequest and
// receives a JSON-encoded nat.PunchOffer.
//
// probe, when non-nil, is the responder's punch half: after returning
// the offer the handler waits until the shared T0 and fires PunchProbe
// at the requester's candidates. nil probe (e.g. in tests) makes the
// handler signaling-only.
type holepunchRPCHandler struct {
	coord *punchCoordinator
	probe nat.ProbeFunc
}

func (h *holepunchRPCHandler) Name() string               { return holepunchRPCHandlerName }
func (h *holepunchRPCHandler) Role() string               { return "system" }
func (h *holepunchRPCHandler) RequiresAuth() bool         { return false }
func (h *holepunchRPCHandler) AllowedAuthTypes() []string { return nil }
func (h *holepunchRPCHandler) Scopes() []string           { return nil }
func (h *holepunchRPCHandler) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeNone
}
func (h *holepunchRPCHandler) AllowedTenants() []string { return nil }

// punchProbeWindow bounds the responder-side probe goroutine: it must
// outlast the wait-until-T0 plus the probe burst, but not linger.
const punchProbeWindow = nat.PunchLeadTime + 2*time.Second

// ExecuteRPC decodes the inbound PunchRequest, builds a signed
// PunchOffer (the request arrived over an authenticated mesh session,
// so buildOfferAuthenticated is correct here), and returns it
// JSON-encoded. When a probe func is wired, it also spawns the
// responder half of the punch — at the shared T0 it opens this node's
// NAT mapping toward the requester so the requester's dial gets
// through. Malformed input yields an unsuccessful response rather than
// a transport error.
func (h *holepunchRPCHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	var pr nat.PunchRequest
	if err := json.Unmarshal(req.Payload, &pr); err != nil {
		return &handlers.RPCResponse{ID: req.ID, Success: false, Error: "holepunch: malformed PunchRequest payload"}, nil
	}
	offer := h.coord.buildOfferAuthenticated(&pr)
	payload, err := json.Marshal(offer)
	if err != nil {
		return &handlers.RPCResponse{ID: req.ID, Success: false, Error: "holepunch: failed to encode PunchOffer"}, nil
	}
	// Responder punch half — runs on its own bounded context: the RPC
	// context ends when this call returns, but the probe must survive
	// until the shared T0.
	if h.probe != nil {
		go func(pr nat.PunchRequest) {
			pctx, cancel := context.WithTimeout(context.Background(), punchProbeWindow)
			defer cancel()
			nat.ExecuteProbe(pctx, &pr, nat.PunchFireTime(&pr), h.probe)
		}(pr)
	}
	return &handlers.RPCResponse{ID: req.ID, Success: true, Payload: payload}, nil
}

// punchAttemptTimeout bounds a whole hole-punch attempt: the signaling
// RPC round-trip + the wait-until-T0 + the simultaneous-dial handshake.
const punchAttemptTimeout = 12 * time.Second

// attemptHolePunch runs the initiator half of a coordinated hole-punch
// for a peer whose direct noise-UDP dial just failed. It signals the
// peer's "holepunch" RPC over that peer's existing mesh session, then
// at the shared T0 fires a simultaneous noise-UDP dial at the offered
// candidates — while the peer, in its RPC handler, fires PunchProbe
// back so both NATs open at once.
//
// On success the punched connection is handed to DialAndAcceptMesh
// exactly like a normal dial and true is returned. Any failure — no
// coordinator, signaling error, notViable method, every candidate
// failed — returns false and the caller keeps the peer's existing path.
func (m *ConnectionManager) attemptHolePunch(ctx context.Context, nodeID, peerRegion string) bool {
	coord := m.rt.punchCoord
	if coord == nil {
		return false
	}
	punchCtx, cancel := context.WithTimeout(ctx, punchAttemptTimeout)
	defer cancel()

	target := aether.NodeID(nodeID)
	offer, t0, err := coord.requestOffer(punchCtx, m.CallViaBidi, target)
	if err != nil {
		log.Printf("[multipath-dial] %s: hole-punch signaling failed: %v", truncID(nodeID), err)
		return false
	}

	dial := func(dctx context.Context, addr net.UDPAddr) (aether.Connection, error) {
		return m.rt.dialNoiseUDP(dctx, aether.Target{NodeID: target, Address: addr.String()})
	}
	result, err := nat.ExecutePunch(punchCtx, offer, t0, dial)
	if err != nil {
		log.Printf("[multipath-dial] %s: hole-punch dial failed: %v", truncID(nodeID), err)
		return false
	}
	if result.NotViable || result.Conn == nil {
		log.Printf("[multipath-dial] %s: hole-punch not viable (method=%v)", truncID(nodeID), offer.Method)
		return false
	}

	baseConn, ok := result.Conn.(*aether.BaseConnection)
	if !ok || baseConn == nil {
		log.Printf("[multipath-dial] %s: hole-punch produced unexpected conn type", truncID(nodeID))
		return false
	}

	m.recordDialSuccess(nodeID, ProtoNoiseUDP)
	bootstrapHost := ""
	m.mu.Lock()
	if peer, ok := m.peers[nodeID]; ok {
		peer.outboundDialed = true
		peer.reconnectDelay = baseCooldown
		bootstrapHost = peer.bootstrapHost
	}
	m.mu.Unlock()

	serviceName := m.peerServiceName(nodeID)
	// Bind the session to the long-lived runtime ctx, not the
	// ephemeral dial ctx (which runEnsure cancels the instant attemptHolePunch
	// returns) — otherwise the freshly hole-punched path is torn down at its
	// first OpenStream, silently defeating coordinated NAT traversal. See E01.
	go m.rt.DialAndAcceptMesh(m.rt.ctx, baseConn.Conn, nodeID, peerRegion, ProtoNoiseUDP, bootstrapHost, serviceName, "", dialBorrowsConnectingState)
	log.Printf("[multipath-dial] %s: hole-punch established noise-UDP path", truncID(nodeID))
	return true
}
