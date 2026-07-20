/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"encoding/json"
	"fmt"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
)

// ladRumorApplyFn is the G3 record-apply callback. It multiplexes by
// the freshness magic prefix: payloads starting with 0xF0 (see
// FreshnessPayloadMagic) go to the FreshnessDeliverer for the
// reach-freshness feedback loop; everything else is unmarshalled as
// a lad.Record and applied to the DirectoryCache.
//
// The prefix discrimination is why G3 can't use a bare protobuf
// codec — a LAD record JSON always starts with '{' (0x7B), so any
// byte outside UTF-8 start bytes (like 0xF0) reliably marks a
// non-record payload. Consumers that add future prefix types
// discriminate here.
func ladRumorApplyFn(c *cache.DirectoryCache, deliverer FreshnessDeliverer) func([]byte) error {
	return func(payload []byte) error {
		if len(payload) > 0 && payload[0] == FreshnessPayloadMagic {
			if deliverer != nil {
				deliverer.Deliver(payload)
			}
			return nil
		}
		var rec lad.Record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("rumor: unmarshal LAD record: %w", err)
		}
		return c.Apply(rec)
	}
}

// ladRumorDedupKey uses the LAD-specific rumor ID for payloads that
// decode as lad.Record, falling back to a raw-payload hash for
// freshness-prefixed or malformed bodies. Keeps dedup keys
// deterministic across initiator/responder on the same payload.
func ladRumorDedupKey(payload []byte) string {
	if len(payload) > 0 && payload[0] == FreshnessPayloadMagic {
		// Freshness messages dedup on body hash (first 32 bytes) —
		// the outer magic prefix is constant across all freshness
		// payloads, so the content after the prefix is what matters.
		n := len(payload)
		if n > 32 {
			n = 32
		}
		return fmt.Sprintf("fresh:%x", payload[:n])
	}
	var rec lad.Record
	if err := json.Unmarshal(payload, &rec); err != nil {
		n := len(payload)
		if n > 16 {
			n = 16
		}
		return fmt.Sprintf("raw:%x", payload[:n])
	}
	return LADRumorID(rec)
}
