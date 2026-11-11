/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/bbmumford/ledger/anchor"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
)

// anchorPublicKeys decodes the anchor public keys a, one per anchor-key
// holder: PublicKey first, then each PublicKeys entry, trimmed, each key once.
// It returns the keys that decode and an error naming every entry that does
// not (not its value). bootstrapFromAnchor refuses a configuration with an
// undecodable key, as it refused an undecodable PublicKey; InitSwarm's
// observer gate logs the error and admits under the keys that decode.
func anchorPublicKeys(a AnchorConfig) ([]ed25519.PublicKey, error) {
	var keys []ed25519.PublicKey
	var bad []string
	seen := map[string]bool{}
	for i, raw := range append([]string{a.PublicKey}, a.PublicKeys...) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		k, err := hex.DecodeString(raw)
		if err != nil || len(k) != ed25519.PublicKeySize {
			name := "PublicKey"
			if i > 0 {
				name = fmt.Sprintf("PublicKeys[%d]", i-1)
			}
			bad = append(bad, name)
			continue
		}
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		keys = append(keys, ed25519.PublicKey(k))
	}
	if len(bad) > 0 {
		return keys, fmt.Errorf("invalid anchor public key in %s (want hex Ed25519, %d bytes)",
			strings.Join(bad, ", "), ed25519.PublicKeySize)
	}
	return keys, nil
}

// anchorPrivateKey decodes a holder's anchor private key (hex, 64-byte
// Ed25519, the form ledger's anchor.NewSigner reads). ok is false when none is
// set or it does not decode.
func anchorPrivateKey(a AnchorConfig) (ed25519.PrivateKey, bool) {
	raw := strings.TrimSpace(a.PrivateKey)
	if raw == "" {
		return nil, false
	}
	k, err := hex.DecodeString(raw)
	if err != nil || len(k) != ed25519.PrivateKeySize {
		return nil, false
	}
	return ed25519.PrivateKey(k), true
}

// anchorEndorsement returns the AnchorEndorsementTag value for this node's
// fleet.peer record issued at issuedAt: its anchor private key's signature
// over its node ID, its identity key and issuedAt. ok is false on a node that
// holds no anchor private key, or no identity yet. PeerPublisher.publishOnce
// calls it on every publish, because the endorsement covers the record's
// IssuedAt.
func (rt *Runtime) anchorEndorsement(issuedAt int64) (string, bool) {
	if rt == nil || rt.identity == nil || len(rt.identity.PublicKey) == 0 {
		return "", false
	}
	priv, ok := anchorPrivateKey(rt.cfg.Anchor)
	if !ok {
		return "", false
	}
	v, err := directory.SignAnchorEndorsement(priv, ports.NodeID(string(rt.identity.NodeID)), rt.identity.PublicKey, issuedAt)
	if err != nil {
		log.Printf("[SWARM] anchor endorsement not signed: %v", err)
		return "", false
	}
	return v, true
}

// AnchorEndorsements implements directory.AnchorEndorsements over the
// fleet.peer records this table holds: each peer whose latest record carries
// an AnchorEndorsementTag that decodes, with the Sig-verified key that signed
// the record and its IssuedAt. directory.Policy.AuthorizeObserver reads it on
// every observer check InitSwarm's gate makes, so a pruned or tombstoned peer
// stops counting at once.
func (t *RoleTable) AnchorEndorsements() map[ports.NodeID]directory.EndorsedRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[ports.NodeID]directory.EndorsedRecord)
	for id, info := range t.infoBy {
		v, ok := info.Tags[directory.AnchorEndorsementTag]
		if !ok || len(info.PubKey) != ed25519.PublicKeySize {
			continue
		}
		e, err := directory.ParseAnchorEndorsement(v)
		if err != nil {
			continue
		}
		var issuedAt int64
		if !info.Updated.IsZero() {
			issuedAt = info.Updated.UnixNano()
		}
		out[ports.NodeID(id)] = directory.EndorsedRecord{
			NodeKey:     append(ed25519.PublicKey(nil), info.PubKey...),
			IssuedAt:    issuedAt,
			Endorsement: e,
		}
	}
	return out
}

// anchorObserverPolicy builds the TrustPolicy InitSwarm's observer gate asks:
// cfg's swarm trust material (swarmTrustPolicy, whose configured Observers it
// keeps) plus the anchor public keys cfg.Anchor lists, reading each peer's
// endorsement from table. A key that does not decode is logged and left out.
func anchorObserverPolicy(cfg Config, table directory.AnchorEndorsements) *directory.Policy {
	pc := swarmTrustPolicy(cfg.SwarmTrustPolicy)
	keys, err := anchorPublicKeys(cfg.Anchor)
	if err != nil {
		log.Printf("[SWARM] observer gate: %v; admitting under the %d keys that decode", err, len(keys))
	}
	reportAnchorKeyList(cfg.Anchor, keys)
	pc.AnchorKeys = keys
	p := directory.NewPolicy(pc)
	p.SetAnchorEndorsements(table)
	return p
}

// reportAnchorKeyList logs what the observer gate's anchor key list keys
// leaves out: every endorsed observer when it is empty, and this node's own
// endorsement when the anchor private key a holds is not listed, which
// peers reading the same list then refuse. anchorObserverPolicy calls it at
// boot and SetAnchorPublicKeys on every reloaded list, so a re-signed
// document is reported as the booted one was.
func reportAnchorKeyList(a AnchorConfig, keys []ed25519.PublicKey) {
	if len(keys) == 0 {
		log.Printf("[SWARM] observer gate: no anchor public key configured, so no endorsed observer is admitted")
	}
	if priv, ok := anchorPrivateKey(a); ok {
		own := priv.Public().(ed25519.PublicKey)
		listed := false
		for _, k := range keys {
			listed = listed || bytes.Equal(k, own)
		}
		if !listed {
			log.Printf("[SWARM] observer gate: this node's anchor key is not among the listed anchor public keys, so peers do not admit its endorsement")
		}
	}
}

// SetAnchorPublicKeys replaces the anchor public keys under which the
// observer gate InitSwarm wired admits an endorsement with publicKeys: hex
// Ed25519 keys, one per anchor-key holder, read as anchorPublicKeys reads
// Config.Anchor.PublicKeys (trimmed, each key once). The composition root
// calls it with the list a reloaded signed mesh document names (meshcfg's
// HolderPublicKeys), as its partition-key reloader calls UpdatePartitionKeys,
// so a holder a re-signed document adds counts, and one it drops stops
// counting, without a restart. An entry that does not decode is left out and
// named (not its value) in the returned error while the keys that decode
// apply, as InitSwarm's gate reads the booted list. It refuses a runtime
// whose InitSwarm has not built the gate, which holds no list to replace.
// The snapshot keys bootstrapFromAnchor read at boot are not changed: a
// bootstrap happens once.
func (rt *Runtime) SetAnchorPublicKeys(publicKeys []string) error {
	si := rt.Swarm()
	if si == nil || si.observers == nil {
		return errors.New("node: no observer gate to update (InitSwarm has not run)")
	}
	keys, err := anchorPublicKeys(AnchorConfig{PublicKeys: publicKeys})
	reportAnchorKeyList(rt.cfg.Anchor, keys)
	si.observers.SetAnchorKeys(keys)
	return err
}

// observerCheck adapts policy.AuthorizeObserver to swarm's observer gate
// (Node.SetObserverRoleCheck), which swarm calls on every observer
// attestation before it counts toward the K-of-N quorum, local ones included,
// with the observer, its Sig-verified key and the attestation's subject.
// The subject is passed on, so AuthorizeObserver refuses a node attesting
// about itself: swarm's own refusal of that (PublishObserverTombstone) binds
// only an honest sender, and a crafted self-attestation reaches this gate.
func observerCheck(policy ports.TrustPolicy) func(swarm.NodeID, []byte, swarm.NodeID) bool {
	var refused atomic.Uint64
	return func(id swarm.NodeID, pub []byte, subject swarm.NodeID) bool {
		err := policy.AuthorizeObserver(context.Background(),
			ports.Principal{NodeID: ports.NodeID(string(id)), PubKey: ed25519.PublicKey(pub)}, ports.NodeID(string(subject)))
		if err != nil {
			if n := refused.Add(1); n <= 10 || n%100 == 0 {
				log.Printf("[SWARM] observer attestation from %s dropped: %v (#%d)", truncID(string(id)), err, n)
			}
			return false
		}
		return true
	}
}

// verifyAnchorSnapshot accepts s when its signature verifies under any of the
// listed anchor public keys. bootstrapFromAnchor calls it on each fetched
// snapshot: each anchor-key holder signs its snapshots with its own anchor
// key, so a snapshot from any listed holder verifies.
func verifyAnchorSnapshot(s *anchor.Snapshot, keys []ed25519.PublicKey) error {
	err := anchor.ErrInvalidSignature
	for _, k := range keys {
		if err = anchor.Verify(s, k); err == nil {
			return nil
		}
	}
	return err
}
