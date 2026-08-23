/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bbmumford/loom/secrets"
)

// RoleSecretsPublisher is the publish-side partner to the takeover engine: a role's current holder
// periodically re-seals its config bundle to the entitled recipient set and publishes it on the
// role's secret topic, so a takeover candidate always holds a current sealed envelope to open the
// instant it must assume the role. Default off — a node starts it only for the roles it holds.
type RoleSecretsPublisher struct {
	rt  *Runtime
	cfg PublisherConfig
}

// PublisherConfig configures the republish loop. Bundle + Recipients are injected because WHAT a
// role publishes (its config) and WHO may open it (the entitled set) are platform concerns, not
// mesh concerns — loom seals and gossips whatever the holder hands it, and the recipient set is
// the same entitlement the takeover engine's Entitled gate authorizes against.
type PublisherConfig struct {
	// Roles this node currently holds and republishes for.
	Roles []string
	// Interval between republishes; default 5m. A candidate that boots mid-interval still gets
	// the current bundle from the next tick (and StartRoleSecretsPublisher publishes once up front).
	Interval time.Duration
	// Bundle returns the current sealed-config bundle + its monotonic epoch for a held role. A
	// higher epoch supersedes a lower one at every recipient (the takeover keeps the max epoch).
	Bundle func(ctx context.Context, role string) (secrets.ConfigBundle, uint64, error)
	// Recipients returns the entitled recipient set for a role — the nodes permitted to assume it.
	// Empty is not an error (no eligible peer yet); nothing is sealed until one exists.
	Recipients func(ctx context.Context, role string) ([]secrets.Recipient, error)
}

// StartRoleSecretsPublisher validates the config and starts the republish loop. Requires InitSwarm
// (it publishes over swarm topics). Failures to build a single role's bundle are logged and
// skipped, never fatal — a publisher that cannot seal one role must not stop republishing the rest.
func (rt *Runtime) StartRoleSecretsPublisher(cfg PublisherConfig) (*RoleSecretsPublisher, error) {
	if rt.swarm == nil || rt.swarm.Node == nil {
		return nil, fmt.Errorf("role secrets publisher: InitSwarm must run first")
	}
	if cfg.Bundle == nil || cfg.Recipients == nil {
		return nil, fmt.Errorf("role secrets publisher: Bundle and Recipients are required")
	}
	if len(cfg.Roles) == 0 {
		return nil, fmt.Errorf("role secrets publisher: no roles to publish")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	p := &RoleSecretsPublisher{rt: rt, cfg: cfg}
	rt.GoCtx("role.secrets.publisher", p.run)
	log.Printf("[ROLE-SECRETS] publishing roles %v every %s", cfg.Roles, cfg.Interval)
	return p, nil
}

func (p *RoleSecretsPublisher) run(ctx context.Context) {
	p.publishAll(ctx) // seal + publish immediately so a fresh candidate need not wait a cadence
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.publishAll(ctx)
		}
	}
}

// Republish forces an immediate re-seal + publish of every held role — call it when a role's config
// changes so candidates receive the new bundle without waiting for the cadence.
func (p *RoleSecretsPublisher) Republish(ctx context.Context) { p.publishAll(ctx) }

func (p *RoleSecretsPublisher) publishAll(ctx context.Context) {
	for _, role := range p.cfg.Roles {
		bundle, epoch, err := p.cfg.Bundle(ctx, role)
		if err != nil {
			log.Printf("[ROLE-SECRETS] bundle for role %q: %v", role, err)
			continue
		}
		recips, err := p.cfg.Recipients(ctx, role)
		if err != nil {
			log.Printf("[ROLE-SECRETS] recipients for role %q: %v", role, err)
			continue
		}
		if len(recips) == 0 {
			continue // no entitled recipient yet — nothing to seal to
		}
		if err := p.rt.PublishRoleSecrets(ctx, role, epoch, bundle, recips); err != nil {
			log.Printf("[ROLE-SECRETS] publish role %q: %v", role, err)
		}
	}
}
