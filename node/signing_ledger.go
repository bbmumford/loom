/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"errors"
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

// ErrTopicReserved is returned when a record is published to a topic the
// receiving DirectoryCache does not retain.
//
// 🛑 `keyops` and `quorum` are ACCEPT-AND-DISCARD. They are registered on the
// cache with merge/key funcs and a
// signed-topic ACL, and this wrapper will sign them — but ladcache's applyCore
// switch ends `default: // ignore other topics for directory view`, so a
// record on either topic Applies WITH NO ERROR and is then invisible to every
// read (measured: ChangesSinceHLC 0, Dump 0, HasRecord false, Fingerprint 0,
// against a member-topic control of 1).
//
// Harmless today — a census over 2,741 .go files found zero producers — and it
// fails OPEN the instant that changes: a key REVOCATION would be signed here,
// accepted by every peer's ACL, gossiped, and silently dropped by every
// receiver. A revocation that vanishes is worse than one that errors.
//
// ladcache is a published dependency and cannot be changed under the freeze;
// this wrapper is the chokepoint we own. Every lad.Ledger in loom is
// signingLedger-wrapped (all four rt.ledger assignments, including the one
// handed to gossip.NewSynchronizer), so refusing here bounds the publish path.
//
// UNBLOCK CONDITION — this is a blocking precondition on FIRST USE, not a
// permanent ban: whoever first needs either topic must land a consumer in the
// same change (a cache that retains it, or a projection fed from swarm, which
// does carry these records), then remove the topic from reservedTopics. Do not
// relax this by deleting the check.
var ErrTopicReserved = errors.New("ledger: topic is reserved — no receiver retains it")

// reservedTopics are publishable-but-not-consumable. See ErrTopicReserved.
var reservedTopics = map[lad.Topic]bool{
	lad.TopicKeyOps: true,
	lad.TopicQuorum: true,
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
	if reservedTopics[rec.Topic] {
		return fmt.Errorf("%w: %q — publishing would be silently discarded by every "+
			"receiver; land a consumer first", ErrTopicReserved, rec.Topic)
	}
	if isSignedTopic(rec.Topic) && len(rec.Signature) == 0 {
		// Fail closed rather than append an unsigned record on a signed
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
	// Checked in a first pass so the batch is refused as a whole: a partial
	// append that dropped only the reserved records would be the same
	// accept-and-discard failure one layer up.
	for i := range records {
		if reservedTopics[records[i].Topic] {
			return fmt.Errorf("%w: %q at index %d — publishing would be silently "+
				"discarded by every receiver; land a consumer first",
				ErrTopicReserved, records[i].Topic, i)
		}
	}
	for i := range records {
		if isSignedTopic(records[i].Topic) && len(records[i].Signature) == 0 {
			// Fail closed on a signed topic with no signing key or an
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
