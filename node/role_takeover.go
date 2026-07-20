/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	swarm "github.com/bbmumford/swarm"

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
}

// claimBody is the JSON body of a role.claim.<role> record.
type claimBody struct {
	Role     string `json:"role"`
	NodeID   string `json:"nodeId"`
	AtUnixMs int64  `json:"atUnixMs"`
}

type roleClaim struct {
	body claimBody
	hlc  uint64
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
	envelopes    map[string]*secrets.Envelope // role → latest sealed bundle
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
	set[string(r.NodeID)] = roleClaim{body: body, hlc: r.HLC}
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
	log.Printf("[TAKEOVER] assumed role %q (claim won, %d/%d coverage before takeover)", role, len(rankClaims(claimSet, 1<<30)), e.cfg.MinReplicas)
	e.mu.Lock()
	delete(e.missingSince, role)
	e.mu.Unlock()
}

func (e *TakeoverEngine) publishClaim(role string) error {
	body, err := json.Marshal(claimBody{
		Role:     role,
		NodeID:   string(e.rt.identity.NodeID),
		AtUnixMs: time.Now().UnixMilli(),
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

// rankClaims orders claimants by (HLC, NodeID) ascending and returns the
// first n node IDs — the deterministic first-N-win rule (pollen claim-set
// semantics: earliest claims win; NodeID breaks HLC ties).
func rankClaims(set map[string]roleClaim, n int) []string {
	type kv struct {
		id string
		c  roleClaim
	}
	all := make([]kv, 0, len(set))
	for id, c := range set {
		all = append(all, kv{id, c})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].c.hlc != all[j].c.hlc {
			return all[i].c.hlc < all[j].c.hlc
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

