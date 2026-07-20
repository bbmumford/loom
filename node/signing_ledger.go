/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"

	lad "github.com/bbmumford/ledger"
)

// signingLedger wraps a lad.Ledger so every Append on a signed topic
// (TopicMember/TopicRole/TopicReach/TopicKeyOps/TopicQuorum) carries a
// valid envelope signature even when the caller forgets. Records that
// arrive already signed (len(Signature) > 0) pass through untouched,
// so reach.Publisher and lad_reach_bridge keep their existing
// signatures.
//
// Without this wrapper the five bare lad.Record{...} Append sites
// scattered through runtime.go + mesh_connection.go are silently
// dropped by every peer's signedTopicACL — VerifyRecord short-circuits
// false on empty AuthorPubKey/Signature, producing the residual
// gossip sig-fail flood observed post-v0.0.371. Channelling every
// signed-topic Append through one signing chokepoint also makes the
// SignRecord field-ordering invariant easier to maintain — only one
// caller needs to get the order right, not five.
type signingLedger struct {
	inner lad.Ledger
	priv  ed25519.PrivateKey
}

// newSigningLedger wraps inner so signed-topic Appends without a
// Signature get one stamped with priv before delegation. priv must be
// the local node's identity private key (rt.identity.PrivateKey).
func newSigningLedger(inner lad.Ledger, priv ed25519.PrivateKey) *signingLedger {
	return &signingLedger{inner: inner, priv: priv}
}

func isSignedTopic(topic lad.Topic) bool {
	switch topic {
	case lad.TopicMember, lad.TopicRole, lad.TopicReach, lad.TopicKeyOps, lad.TopicQuorum:
		return true
	}
	return false
}

func (s *signingLedger) Head(ctx context.Context) (lad.CausalWatermark, error) {
	return s.inner.Head(ctx)
}

func (s *signingLedger) Append(ctx context.Context, rec lad.Record) error {
	if isSignedTopic(rec.Topic) && len(rec.Signature) == 0 {
		// MESH-H11: fail closed rather than append an unsigned record on a signed
		// topic — peers' signed-topic ACL would silently drop it, exactly the
		// regression this wrapper exists to prevent. Guard the key up front
		// (ed25519.Sign panics on a nil key) and re-check the signature landed.
		if len(s.priv) == 0 {
			return fmt.Errorf("signing ledger: cannot sign %s record — no private key", rec.Topic)
		}
		lad.SignRecord(&rec, s.priv)
		if len(rec.Signature) == 0 {
			return fmt.Errorf("signing ledger: %s record still unsigned after SignRecord", rec.Topic)
		}
	}
	return s.inner.Append(ctx, rec)
}

func (s *signingLedger) BatchAppend(ctx context.Context, records []lad.Record) error {
	for i := range records {
		if isSignedTopic(records[i].Topic) && len(records[i].Signature) == 0 {
			// MESH-H11: fail closed on a signed topic with no signing key or an
			// empty signature — see Append.
			if len(s.priv) == 0 {
				return fmt.Errorf("signing ledger: cannot sign %s record — no private key", records[i].Topic)
			}
			lad.SignRecord(&records[i], s.priv)
			if len(records[i].Signature) == 0 {
				return fmt.Errorf("signing ledger: %s record still unsigned after SignRecord", records[i].Topic)
			}
		}
	}
	return s.inner.BatchAppend(ctx, records)
}

func (s *signingLedger) Stream(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, error) {
	return s.inner.Stream(ctx, from, topics)
}

func (s *signingLedger) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return s.inner.Snapshot(ctx)
}
