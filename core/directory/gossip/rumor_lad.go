/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"fmt"

	lad "github.com/bbmumford/ledger"
)

// LADRumorID generates a dedup key for a LAD record. Used by the
// rumor dedup table so the same record broadcast by two peers
// doesn't re-apply twice to the local cache.
//
// Key includes Seq and LamportClock alongside Timestamp.UnixNano. Two
// records with the same (topic, tenant, node) emitted in the same
// nanosecond — which happens during burst updates (latency flood,
// multipath flap, reach update cascade) — previously collapsed onto
// the same key and silently lost the later writes. The cache is
// last-writer-wins per logical key + IBLT reconcile self-heals so
// permanent loss was avoided, but rumor apply latency increased.
// Was the H11 finding. No wire change — key is local-only.
func LADRumorID(rec lad.Record) string {
	return fmt.Sprintf("%s:%s:%s:%d:%d:%d",
		rec.Topic, rec.TenantID, rec.NodeID,
		rec.Timestamp.UnixNano(), rec.Seq, rec.LamportClock)
}

// FreshnessPayloadMagic is the first byte that marks a reach-freshness
// payload (as opposed to a LAD record JSON object). Matches
// node.FreshnessMagicPrefix — duplicated here to avoid an import cycle
// (mesh/node imports this package).
const FreshnessPayloadMagic byte = 0xF0

// FreshnessDeliverer receives magic-prefixed freshness payloads from the
// rumor pipeline and fans them out to FreshnessBus subscribers. The
// node runtime wires one instance and passes it into the responder
// loop; the rumor apply fn (frame_handlers.go) routes 0xF0-prefixed
// payloads here instead of to cache.Apply.
type FreshnessDeliverer interface {
	Deliver(payload []byte)
}
