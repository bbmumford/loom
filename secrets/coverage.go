/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package secrets

import (
	"context"
	"time"

	"github.com/bbmumford/loom/ports"
)

// CoverageConfig tunes missing-role detection (plan §1.3).
type CoverageConfig struct {
	// MinReplicas is the per-role floor; coverage below it (not just zero)
	// counts as missing. Default 1.
	MinReplicas int

	// CorroborationWindow is how long a role must stay under-covered before
	// RoleMissing fires. Together with quorum corroboration it prevents a
	// single partition-blind node from triggering a spurious takeover.
	// Default 60s (matches the observer-silence threshold the tombstone
	// path uses).
	CorroborationWindow time.Duration

	// CorroborationQuorum is the K in K-of-N: distinct authorized observers
	// that must independently report the gap within the window. Default 2
	// (the swarm observer-tombstone default).
	CorroborationQuorum int
}

// RoleMissing is the coverage-gap event that arms takeover. It is only ever
// acted on through the claim-set (ClaimTopic) — never directly.
type RoleMissing struct {
	Role             string
	ObservedAtUnixMs int64
	// Corroborators are the authorized observers that reported the gap
	// (TrustPolicy.AuthorizeObserver-vetted; self-advertised nodes don't
	// count toward quorum).
	Corroborators []ports.NodeID
}

// CoverageEvaluator watches LiveDirectory role coverage against the
// RoleConfig priority order and emits corroborated RoleMissing events. It
// consumes the same liveness signal the tombstone path uses (gossip-silence
// observers), so a departed anchor's roles surface here as its records
// expire — durable tombstone GC stays governed by journal checkpoints, not
// wall-clock deletion.
type CoverageEvaluator interface {
	// Missing returns the currently corroborated coverage gaps.
	Missing(ctx context.Context) ([]RoleMissing, error)

	// Subscribe streams RoleMissing events as they become corroborated.
	// Cancel ctx to unsubscribe.
	Subscribe(ctx context.Context) (<-chan RoleMissing, error)
}
