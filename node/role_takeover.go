/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// TakeoverConfig tunes the Phase-1 missing-role detection + takeover engine.
type TakeoverConfig struct {
	// Roles this node guards (typically capabilities.RoleConfig
	// PriorityOrder ∩ roles this node has an activator + entitlement for).
	Roles []string

	// MinReplicas per role; coverage below it counts as missing. Default 1.
	MinReplicas int

	// CorroborationWindow: how long a role must stay under-covered before
	// takeover arms. With the claim-set it prevents a transient gossip gap
	// or a partition-blind node from triggering churn. Default 60s
	// (matches the observer-silence threshold the tombstone path uses).
	CorroborationWindow time.Duration

	// ClaimSettle: how long after publishing a claim before ranking
	// resolves. Every candidate waits the same settle, so claims gossip
	// fleet-wide before anyone activates. Default 10s.
	ClaimSettle time.Duration

	// MaxWinners: how many claimants may activate (usually MinReplicas).
	MaxWinners int

	// Entitled gates ENVELOPE OPENING for this node per role — wire
	// TrustPolicy.AuthorizeSecretRecipient for the local principal. It
	// runs before key material is touched (the diststore fail-closed
	// contract). Required.
	Entitled func(role string) error

	// CheckInterval between coverage sweeps. Default 15s.
	CheckInterval time.Duration

	// ChainRung is this node's position in the ordered failover chain:
	// "The failover chain is explicit and ordered, ending in thin
	// applications acting as emergency thick ones — so the last rung is
	// degraded operation, not absence."
	//
	// 1 is the MOST-preferred rung; higher numbers are later fallbacks. A
	// node only wins a role when no lower-numbered rung claims it, which is
	// §6's "the chain descends only as far as needed".
	//
	// Default: ChainDepth — the LAST rung. An unconfigured node is the LEAST
	// preferred candidate, never the most. Defaulting to 1 would make every
	// node that forgot to set this outrank every node that set it
	// deliberately, which is the absent-sorts-as-best failure this codebase
	// keeps hitting.
	ChainRung int

	// ChainDepth bounds the chain — the "failover chain depth"
	// as a limit and states "All fail-closed", so a rung beyond the depth is
	// clamped to the last rung rather than being trusted. Default 1 — a
	// single-rung chain, where every node ranks equally on rung.
	ChainDepth int
}

// rungUnset is the rank given to a claim carrying no chain rung — an older
// node, or a hand-crafted record. It sorts AFTER every declared rung.
//
// 🔴 This is deliberate and load-bearing. `Rung` is `omitempty` on the wire, so
// a claim carrying no rung decodes to 0 — and 0 would be the
// MOST-preferred rung. Absence must not outrank measurement, so an unset rung
// is treated as the least-preferred candidate. Fail-closed, per §10.
const rungUnset = 1 << 30

// claimBody is the JSON body of a role.claim.<role> record.
type claimBody struct {
	Role     string `json:"role"`
	NodeID   string `json:"nodeId"`
	AtUnixMs int64  `json:"atUnixMs"`
	// Rung is the claimant's failover-chain position. omitempty for
	// wire compatibility with nodes that predate the chain; see rungUnset for
	// how an absent value is ranked.
	Rung int `json:"rung,omitempty"`
	// MinHolders is the claimant's declared cardinality FLOOR for the role —
	// "at least N holders". omitempty, same wire-compat reason as Rung.
	//
	// 🔑 WHY IT IS HERE NOW RATHER THAN LATER, AND WHY IT DOES NOT RANK.
	// Rung and MinHolders ship together: both qualify the same claim record, and
	// adding them in separate releases would change the wire format twice. They
	// are the same missing idea,
	// so both land in ONE change while nothing is shipped and "twice" is still
	// avoidable.
	//
	// 🛑 OPEN QUESTION — "decide whether any role is
	// genuinely exclusive, and if so, what fences it." Giving this field ranking
	// semantics would answer that by implementation. So it is CARRIED and
	// COMPARED (see claimFloorDisagreement) and it does NOT participate in
	// rankClaims. That boundary is deliberate: the wire contract is frozen, the
	// policy question stays open.
	MinHolders int `json:"minHolders,omitempty"`
}

type roleClaim struct {
	body claimBody
	hlc  uint64
	// rung is body.Rung normalised through normaliseRung — never the raw
	// wire value, so an absent or nonsensical rung cannot rank as preferred.
	rung int
	// minHolders is body.MinHolders normalised through normaliseFloor. Recorded
	// for disagreement detection only — rankClaims never reads it ("the claim
	// floor and precedence rung cannot manufacture
	// a ceiling").
	minHolders int
	// sig is the record's Ed25519 signature, carried through from
	// swarm.Record.Sig so the ranker can satisfy the "deterministic
	// SIGNED claim order".
	//
	// 🔑 IT IS SAFE TO ORDER ON THIS AND ON NOTHING WEAKER. swarm verifies the
	// signature before delivery (swarm@v0.0.8 sig.go:95, ed25519.Verify over
	// signableBytes(r)), so a delivered record's Sig is attested, not
	// claimant-asserted. The previous final tie-break was the node ID — which
	// the claimant CHOOSES, so a node could bias which claim won a rung/HLC tie
	// simply by picking a low-sorting ID. A signature cannot be ground toward a
	// target without the private key.
	//
	// Empty for a claim constructed in-process rather than ingested from a
	// record; see rankClaims for why absent sorts LAST.
	sig []byte
}

// floorUnset marks a claim that declared no cardinality floor — an older peer,
// or one that has not been configured with MinReplicas.
//
// It is a distinct value rather than 0 because "declared no floor" and
// "declared a floor of zero" are different facts, and collapsing them is the
// absent-is-an-ordinary-value mistake this file already avoids for Rung.
const floorUnset = -1

// normaliseFloor maps a wire floor onto its recorded value, failing closed in
// the sense a limit requires: a missing, zero or negative
// floor is recorded as UNSET rather than as a satisfied floor of zero. A floor
// of zero would mean "no holders required", which is the one reading that could
// let a role go uncovered without anything noticing.
func normaliseFloor(f int) int {
	if f <= 0 {
		return floorUnset
	}
	return f
}

// claimFloorDisagreement reports whether a peer's declared cardinality floor
// contradicts this node's, and is the ONE thing MinHolders does today.
//
// A carried-but-unread field is the "mechanism that exists, is wired, and never
// fires" shape this lane has filed four findings on today, so the field earns
// its place on the wire by surfacing a real operational fault: two nodes
// guarding one role while disagreeing about how many holders it needs is a
// configuration split-brain that is otherwise invisible until the role is
// under-covered in production.
//
// An UNSET floor on either side is not a disagreement — it is silence, and
// silence is not contradiction (an older peer predates the field entirely).
func claimFloorDisagreement(local, remote int) bool {
	if local == floorUnset || remote == floorUnset {
		return false
	}
	return local != remote
}

// normaliseRung maps a wire rung onto its ranking value, failing closed: a
// missing, zero or negative rung becomes rungUnset (least preferred).
func normaliseRung(r int) int {
	if r <= 0 {
		return rungUnset
	}
	return r
}

// resolveChainPosition applies the fail-closed bound to this
// node's declared chain position, returning (rung, depth).
//
// Extracted from StartTakeover deliberately: mutation showed the clamp was
// unreachable from any test, because StartTakeover needs a live swarm Node.
// An untestable safety default is not a safety default, and this
// is the highest-consequence one in the file — see the rung==1 case below.
// 🔴 THE RUNG IS A GLOBAL CLASS AND MUST NOT BE CLAMPED TO A LOCAL BOUND.
//
// ChainDepth is PRIVATE — it is not on the claim record (see claimBody), is
// never negotiated and is never validated against a peer. Clamping the rung to
// it converted a GLOBAL CLASS into a LOCAL INDEX, so published rungs from
// different nodes were not comparable:
//
//	node A: ChainRung 3, ChainDepth 4   → published Rung 3
//	node B: ChainRung 3, ChainDepth 0   → published Rung 1  ← MOST preferred
//
// A thin application deliberately placed at the last rung, whose depth was left
// at its zero value, published itself as class 1 and beat every correctly
// configured thick node at the comparator — where rung returns before HLC is
// consulted — so a dormant rung outranked a live one, and a cross-node
// decision was driven by a value only one node can see.
//
// The rung is a CLASS, so it is now published exactly as declared:
//
//   - undeclared (≤0) publishes ABSENT, which every peer's normaliseRung maps
//     to rungUnset and therefore sorts LAST.
//   - a rung past the local depth is NOT clamped down. A larger rung sorts
//     later, so an over-range value fails toward LESS preference; clamping it
//     toward depth was what made a misconfiguration more preferred.
//
// depth is still returned, but it now bounds only the local "am I the last
// rung" log note — never the value that goes on the wire.
func resolveChainPosition(rung, depth int) (int, int) {
	if depth <= 0 {
		depth = 1 // single-rung chain: every node ranks equally on rung
	}
	if rung < 0 {
		rung = 0 // a negative declaration is no declaration; publish absent
	}
	return rung, depth
}

// TakeoverEngine implements plan §1.3–§1.5: watch coverage via the role
// table, corroborate over a window, claim via the role.claim.<role>
// claim-set (first-N-by-HLC+NodeID win — the anti-thundering-herd
// mechanism), then decrypt the role.secrets.<role> envelope and activate.
// Every decision is deterministic-local ("same view, same decision") — no
// leader.
type TakeoverEngine struct {
	rt     *Runtime
	cfg    TakeoverConfig
	opener secrets.Opener

	mu           sync.Mutex
	envelopes    map[string]*secrets.Envelope    // role → latest sealed bundle
	claims       map[string]map[string]roleClaim // role → claimant nodeID → claim
	missingSince map[string]time.Time
	claimedAt    map[string]time.Time
	unsubs       []swarm.Unsubscribe
}

// StartTakeover wires and starts the takeover engine. Requires InitSwarm
// to have run (the engine subscribes to swarm topics). opener opens
// role.secrets envelopes with this node's identity key; build it with
// secrets.NewOpener(pub, priv, entitledForRole) — the engine passes
// cfg.Entitled per role at open time via the config gate.
func (rt *Runtime) StartTakeover(cfg TakeoverConfig, opener secrets.Opener) (*TakeoverEngine, error) {
	if rt.swarm == nil || rt.swarm.Node == nil {
		return nil, fmt.Errorf("takeover: InitSwarm must run first")
	}
	if opener == nil {
		return nil, fmt.Errorf("takeover: nil opener")
	}
	if cfg.Entitled == nil {
		return nil, fmt.Errorf("takeover: nil entitlement gate (wire TrustPolicy.AuthorizeSecretRecipient)")
	}
	if len(cfg.Roles) == 0 {
		return nil, fmt.Errorf("takeover: no roles to guard")
	}
	if cfg.MinReplicas <= 0 {
		cfg.MinReplicas = 1
	}
	if cfg.MaxWinners <= 0 {
		cfg.MaxWinners = cfg.MinReplicas
	}
	if cfg.CorroborationWindow <= 0 {
		cfg.CorroborationWindow = 60 * time.Second
	}
	if cfg.ClaimSettle <= 0 {
		cfg.ClaimSettle = 10 * time.Second
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 15 * time.Second
	}
	// Chain bounds, fail closed.
	cfg.ChainRung, cfg.ChainDepth = resolveChainPosition(cfg.ChainRung, cfg.ChainDepth)

	e := &TakeoverEngine{
		rt:           rt,
		cfg:          cfg,
		opener:       opener,
		envelopes:    map[string]*secrets.Envelope{},
		claims:       map[string]map[string]roleClaim{},
		missingSince: map[string]time.Time{},
		claimedAt:    map[string]time.Time{},
	}

	node := rt.swarm.Node
	for _, role := range cfg.Roles {
		role := role
		unsubEnv, err := node.Subscribe(swarm.Topic(secrets.Topic(role)), func(r swarm.Record) error {
			e.onSecretRecord(role, r)
			return nil
		})
		if err != nil {
			e.stopSubs()
			return nil, fmt.Errorf("takeover: subscribe secrets %q: %w", role, err)
		}
		e.unsubs = append(e.unsubs, unsubEnv)

		unsubClaim, err := node.Subscribe(swarm.Topic(secrets.ClaimTopic(role)), func(r swarm.Record) error {
			e.onClaimRecord(role, r)
			return nil
		})
		if err != nil {
			e.stopSubs()
			return nil, fmt.Errorf("takeover: subscribe claims %q: %w", role, err)
		}
		e.unsubs = append(e.unsubs, unsubClaim)
	}

	rt.takeover = e
	rt.Go("role.takeover", e.run)
	log.Printf("[TAKEOVER] guarding roles %v (minReplicas=%d window=%s settle=%s)",
		cfg.Roles, cfg.MinReplicas, cfg.CorroborationWindow, cfg.ClaimSettle)
	return e, nil
}

// Takeover returns the running engine, or nil.
func (rt *Runtime) Takeover() *TakeoverEngine { return rt.takeover }

// RoleTakeoverConfig is the default-off Config seam that arms leaderless role takeover after
// InitSwarm. A nil Config.RoleTakeover leaves takeover off (current behaviour); a non-nil one
// starts the engine for this node.
//
// No role is exclusive: multiple nodes may hold a role, so the engine ranks claims and covers
// a shortfall but never fences a winner. Activation is gated per role by Policy — an unseeded
// Policy entitles nothing, so enabling takeover without also seeding role entitlements fails
// closed (the engine arms but activates no role).
type RoleTakeoverConfig struct {
	// Roles this node may take over when under-covered.
	Roles []string
	// MinReplicas per role; coverage below it is a shortfall. Default 1.
	MinReplicas int
	// Policy is the entitlement authority: role takeover opens a role's sealed config bundle
	// only when Policy.AuthorizeSecretRecipient admits THIS node for that role. Required —
	// a nil Policy is fail-closed (takeover is not armed).
	Policy ports.TrustPolicy
}

// armRoleTakeover starts the takeover engine when Config.RoleTakeover is set. Called at the
// end of InitSwarm (the engine subscribes to swarm topics, which requires the node wired).
// Failures are logged and non-fatal: a node that cannot arm takeover still runs — it simply
// does not guard roles, which is the same as takeover being off.
func (rt *Runtime) armRoleTakeover() {
	c := rt.cfg.RoleTakeover
	if c == nil {
		return // default: off
	}
	if c.Policy == nil {
		log.Printf("[TAKEOVER] not armed: RoleTakeover set but Policy is nil (fail-closed)")
		return
	}
	if len(c.Roles) == 0 {
		log.Printf("[TAKEOVER] not armed: RoleTakeover set but no roles listed")
		return
	}
	if rt.identity == nil || rt.identity.PrivateKey == nil {
		log.Printf("[TAKEOVER] not armed: node identity not set")
		return
	}
	// The local principal the entitlement gate authorizes per role.
	self := ports.Principal{NodeID: ports.NodeID(string(rt.identity.NodeID)), PubKey: rt.identity.PublicKey}
	entitled := func(role string) error {
		return c.Policy.AuthorizeSecretRecipient(rt.Context(), self, role)
	}
	// The opener decrypts a role's sealed bundle with this node's identity key; the per-role
	// entitlement is applied by the engine via cfg.Entitled, so the opener gate is a pass.
	opener := secrets.NewOpener(rt.identity.PublicKey, rt.identity.PrivateKey, func() error { return nil })
	if _, err := rt.StartTakeover(TakeoverConfig{
		Roles:       c.Roles,
		MinReplicas: c.MinReplicas,
		Entitled:    entitled,
	}, opener); err != nil {
		log.Printf("[TAKEOVER] not armed: %v", err)
	}
}

// PublishRoleSecrets seals a role's config bundle to the recipient set and
// publishes it on role.secrets.<role>. Called by the role's CURRENT config
// holder (the publisher). Recipients MUST be TrustPolicy-vetted by the
// caller — pass the entitled set, never raw capability advertisers.
func (rt *Runtime) PublishRoleSecrets(ctx context.Context, role string, epoch uint64, bundle secrets.ConfigBundle, recipients []secrets.Recipient) error {
	if rt.swarm == nil || rt.swarm.Node == nil {
		return fmt.Errorf("publish role secrets: InitSwarm must run first")
	}
	env, err := secrets.NewSealer().Seal(ctx, role, epoch, bundle, recipients)
	if err != nil {
		return err
	}
	body, err := secrets.EncodeEnvelope(env)
	if err != nil {
		return err
	}
	return rt.swarm.Node.Publish(swarm.Topic(secrets.Topic(role)), body)
}

func (e *TakeoverEngine) stopSubs() {
	for _, u := range e.unsubs {
		if u != nil {
			u()
		}
	}
	e.unsubs = nil
}

func (e *TakeoverEngine) onSecretRecord(role string, r swarm.Record) {
	if r.Tombstone {
		e.mu.Lock()
		delete(e.envelopes, role)
		e.mu.Unlock()
		return
	}
	env, err := secrets.DecodeEnvelope(r.Body)
	if err != nil || env.Role != role {
		return
	}
	e.mu.Lock()
	cur := e.envelopes[role]
	if cur == nil || env.Epoch >= cur.Epoch {
		e.envelopes[role] = env
	}
	e.mu.Unlock()
}

func (e *TakeoverEngine) onClaimRecord(role string, r swarm.Record) {
	e.mu.Lock()
	defer e.mu.Unlock()
	set := e.claims[role]
	if set == nil {
		set = map[string]roleClaim{}
		e.claims[role] = set
	}
	if r.Tombstone {
		delete(set, string(r.NodeID))
		return
	}
	var body claimBody
	if err := json.Unmarshal(r.Body, &body); err != nil || body.Role != role {
		return
	}
	claim := roleClaim{
		body:       body,
		hlc:        r.HLC,
		rung:       normaliseRung(body.Rung),
		minHolders: normaliseFloor(body.MinHolders),
		// Carry the verified signature so the ranker can tie-break on
		// signed order. Copied, not aliased — r.Body/r.Sig belong to the
		// delivering subscriber and must not be retained by reference.
		sig: append([]byte(nil), r.Sig...),
	}
	// The floor is carried but never ranked. Its one
	// job is to make a configuration split-brain visible BEFORE the role is
	// under-covered in production, rather than after.
	if claimFloorDisagreement(normaliseFloor(e.cfg.MinReplicas), claim.minHolders) {
		log.Printf("[TAKEOVER] role %q: claimant %s declares minHolders=%d but this node is configured for %d — "+
			"the two disagree about how many holders the role needs; coverage decisions on this node use ITS OWN value",
			role, string(r.NodeID), claim.minHolders, e.cfg.MinReplicas)
	}
	set[string(r.NodeID)] = claim
}

func (e *TakeoverEngine) run() {
	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()
	defer e.stopSubs()
	for {
		select {
		case <-e.rt.ctx.Done():
			return
		case <-ticker.C:
			for _, role := range e.cfg.Roles {
				e.evaluateRole(role)
			}
		}
	}
}

// evaluateRole is one deterministic-local pass of the §1.3/§1.4 state
// machine for a role.
func (e *TakeoverEngine) evaluateRole(role string) {
	rt := e.rt
	selfID := string(rt.identity.NodeID)
	covered := len(rt.LookupRoleViaSwarm(role, ""))

	e.mu.Lock()
	envelope := e.envelopes[role]
	claimSet := e.claims[role]
	missingSince, wasMissing := e.missingSince[role]
	claimedAt, hasClaimed := e.claimedAt[role]
	e.mu.Unlock()

	now := time.Now()

	// Covered: disarm, and withdraw a pending claim we no longer need
	// (an ACTIVE role we won stays active — handback is an operator/
	// coverage-pressure decision, not automatic flapping).
	if covered >= e.cfg.MinReplicas {
		e.mu.Lock()
		delete(e.missingSince, role)
		e.mu.Unlock()
		if hasClaimed && !rt.roleActivation.isActive(role) {
			e.withdrawClaim(role)
		}
		return
	}

	// Under-covered: corroborate over the window before arming.
	if !wasMissing {
		e.mu.Lock()
		e.missingSince[role] = now
		e.mu.Unlock()
		dbgTakeover.Printf("role %s under-covered (%d/%d) — corroboration window started", role, covered, e.cfg.MinReplicas)
		return
	}
	if now.Sub(missingSince) < e.cfg.CorroborationWindow {
		return
	}

	// Armed. Can this node act at all?
	if rt.roleActivation.isActive(role) {
		return // we already carry it
	}
	if !rt.HasRoleActivator(role) || envelope == nil {
		return // nothing to activate with (no activator / no sealed bundle)
	}
	if err := e.cfg.Entitled(role); err != nil {
		dbgTakeover.Printf("role %s missing but this node is not entitled: %v", role, err)
		return
	}

	// Claim once, then wait for the settle.
	if !hasClaimed {
		if err := e.publishClaim(role); err != nil {
			log.Printf("[TAKEOVER] claim publish failed for %s: %v", role, err)
		}
		return
	}
	if now.Sub(claimedAt) < e.cfg.ClaimSettle {
		return
	}

	// Rank the claim set deterministically: (HLC, NodeID) ascending —
	// first MaxWinners activate; everyone computes the same ranking from
	// the same gossiped claims, so no coordination round is needed.
	winners := rankClaims(claimSet, e.cfg.MaxWinners)
	selfWins := false
	for _, w := range winners {
		if w == selfID {
			selfWins = true
			break
		}
	}
	if !selfWins {
		dbgTakeover.Printf("role %s: lost claim ranking (winners=%v) — withdrawing", role, winners)
		e.withdrawClaim(role)
		return
	}

	// Winner: decrypt + activate.
	bundle, err := e.opener.Open(rt.ctx, envelope)
	if err != nil {
		log.Printf("[TAKEOVER] %s: envelope open failed (withdrawing claim so another winner proceeds): %v", role, err)
		e.withdrawClaim(role)
		return
	}
	if err := rt.ActivateRole(rt.ctx, role, bundle); err != nil {
		log.Printf("[TAKEOVER] %s: activation failed (withdrawing claim): %v", role, err)
		e.withdrawClaim(role)
		return
	}
	// A takeover event records "which node assumed what and
	// WHY IT WAS ELIGIBLE", so the rung is part of the event rather than
	// something an operator has to infer from config.
	rungNote := fmt.Sprintf("rung %d/%d", e.cfg.ChainRung, e.cfg.ChainDepth)
	if e.cfg.ChainRung >= e.cfg.ChainDepth && e.cfg.ChainDepth > 1 {
		// The last rung is degraded operation, not absence — the chain is
		// meant to REACH this state, so it must be visible when it happens.
		// A silent emergency-thick takeover looks identical to a healthy one.
		rungNote += " EMERGENCY-THICK (last rung — no nearer rung claimed)"
	}
	log.Printf("[TAKEOVER] assumed role %q (claim won, %s, %d/%d coverage before takeover)",
		role, rungNote, len(rankClaims(claimSet, 1<<30)), e.cfg.MinReplicas)
	e.mu.Lock()
	delete(e.missingSince, role)
	e.mu.Unlock()
}

func (e *TakeoverEngine) publishClaim(role string) error {
	body, err := json.Marshal(claimBody{
		Role:     role,
		NodeID:   string(e.rt.identity.NodeID),
		AtUnixMs: time.Now().UnixMilli(),
		Rung:     e.cfg.ChainRung,
		// The declared cardinality FLOOR, from this node's own configuration.
		// Both qualifiers travel in this one record.
		MinHolders: e.cfg.MinReplicas,
	})
	if err != nil {
		return err
	}
	if err := e.rt.swarm.Node.Publish(swarm.Topic(secrets.ClaimTopic(role)), body); err != nil {
		return err
	}
	e.mu.Lock()
	e.claimedAt[role] = time.Now()
	e.mu.Unlock()
	return nil
}

// withdrawClaim releases our claim slot (claim-set release = tombstone).
func (e *TakeoverEngine) withdrawClaim(role string) {
	if err := e.rt.swarm.Node.PublishTombstone(swarm.Topic(secrets.ClaimTopic(role))); err != nil {
		log.Printf("[TAKEOVER] claim withdraw failed for %s: %v", role, err)
		return
	}
	e.mu.Lock()
	delete(e.claimedAt, role)
	e.mu.Unlock()
}

// rankClaims orders claimants by (RUNG, HLC, SIGNED BYTES, NodeID) ascending
// and returns the first n node IDs — the deterministic first-N-win rule
// (pollen claim-set semantics: earliest claims win).
//
// THE SIGNATURE, NOT THE NODE ID, BREAKS AN HLC TIE. A node CHOOSES its own
// ID, so breaking ties on NodeID lets it win a contested takeover by picking a
// low-sorting one. The signature is verified by swarm before delivery and
// cannot be steered without the private key. NodeID serves only as the final
// fallback so the ordering stays total when signatures are equal or absent.
//
// 🔑 RUNG DOMINATES. The failover chain is explicit and ordered, so
// a nearer rung outranks an earlier claim: a rung-1 node that claims late
// still beats a rung-3 node that claimed first. That is §6's "the chain
// descends only as far as needed" — the mesh reaches for the last rung only
// when no nearer one is claiming, and the last rung is emergency-thick
// operation rather than absence (acceptance criterion 7).
//
// HLC remains the tie-break WITHIN a rung, so the anti-thundering-herd
// property is unchanged for a flat (single-rung) chain — which is exactly the
// flat behaviour, and what ChainDepth defaults to.
//
// An unset rung normalises to rungUnset and therefore sorts last; see the
// constant. Absence must never outrank a declared position.
func rankClaims(set map[string]roleClaim, n int) []string {
	type kv struct {
		id string
		c  roleClaim
	}
	all := make([]kv, 0, len(set))
	for id, c := range set {
		if c.rung == 0 {
			// A claim built directly rather than through onClaimRecord (tests,
			// future call sites) has never been normalised. Do it here so no
			// path can smuggle an un-normalised zero into the comparator.
			c.rung = normaliseRung(c.body.Rung)
		}
		all = append(all, kv{id, c})
	}
	// Ranking is rung first, then deterministic signed claim order;
	// equal-version tie-breaks use signed bytes.
	//
	// Order: rung → HLC (the version) → SIGNED BYTES → node ID.
	//
	// 🔴 WHY THE SIGNATURE HAD TO DISPLACE THE NODE ID AS THE DECIDING TIE-BREAK.
	// Rung and HLC can legitimately tie. Before this, the decision then fell to
	// `id` — the claimant's own node ID — so a node could win contested
	// takeovers by choosing a low-sorting ID. That is a chooseable input
	// deciding who assumes a service role. The signature is verified by swarm
	// before delivery and cannot be steered without the private key.
	//
	// 🔑 AN UNSIGNED CLAIM SORTS LAST, NEVER FIRST. bytes.Compare would rank an
	// empty signature ahead of every real one — the "absent sorts as best"
	// inversion this file already guards against for rung (see rungUnset) and
	// for the floor (see floorUnset). A claim with no signature is one whose
	// origin was never attested, so it is the least preferred candidate, not
	// the most. Ties among equally-unsigned claims fall through to the node ID
	// so the ordering stays total and stable.
	sort.Slice(all, func(i, j int) bool {
		if all[i].c.rung != all[j].c.rung {
			return all[i].c.rung < all[j].c.rung
		}
		if all[i].c.hlc != all[j].c.hlc {
			return all[i].c.hlc < all[j].c.hlc
		}
		iSigned, jSigned := len(all[i].c.sig) > 0, len(all[j].c.sig) > 0
		if iSigned != jSigned {
			return iSigned // an attested claim outranks an unattested one
		}
		if c := bytes.Compare(all[i].c.sig, all[j].c.sig); c != 0 {
			return c < 0
		}
		return all[i].id < all[j].id
	})
	if n > len(all) {
		n = len(all)
	}
	out := make([]string, 0, n)
	for _, kv := range all[:n] {
		out = append(out, kv.id)
	}
	return out
}

// isActive reports whether a role is runtime-activated (manager-internal).
func (m *roleActivationManager) isActive(role string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[role]
}
