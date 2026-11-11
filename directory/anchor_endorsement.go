/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bbmumford/loom/ports"
)

// MaxIssuedAtSkew is how far ahead of this node's clock a fleet.peer record's
// IssuedAt may be dated and still be held. node's RoleTable.onRecord refuses a
// peer's record dated later, and Policy.AuthorizeObserver neither admits nor
// counts a record dated later than its own clock plus this, because the
// freshest rule (outranks) ranks records by IssuedAt: unbounded, a record
// from a machine whose clock ran fast outranks every later machine of the
// same holder until the receiver's clock reaches it. It is the bound swarm's
// inbound gate places on the HLC of the same records (Config.MaxClockSkew,
// 10 minutes unset, which node's InitSwarm leaves unset), and a publisher
// dates IssuedAt and its HLC by the same clock, so it refuses no record from
// a publisher that swarm's gate admits.
const MaxIssuedAtSkew = 10 * time.Minute

// AnchorEndorsementTag is the Capabilities.tags key under which a node's
// fleet.peer record carries its anchor endorsement, as
// "anchor_endorsement=<anchor public key hex>.<signature hex>". node's
// PeerPublisher writes it on an anchor-key holder's records and node's
// RoleTable reads it back for Policy.AuthorizeObserver. It rides a tag because
// swarm's PeerRecord has no endorsement field and the record shape is swarm's,
// shared by every peer on the mesh.
const AnchorEndorsementTag = "anchor_endorsement"

// anchorEndorsementDomain separates the endorsement's signed bytes from every
// other message an anchor key signs (ledger's snapshot signatures sign JSON
// snapshot content with the same key).
const anchorEndorsementDomain = "loom.anchor.endorsement.v1\x00"

// AnchorEndorsement is an anchor-key holder's signature, made with its anchor
// private key, over a node's ID, the identity key that signs the node's
// fleet.peer record, and that record's IssuedAt. Policy.AuthorizeObserver
// admits a node as an observer only when its endorsement verifies under an
// anchor public key the policy was configured with.
type AnchorEndorsement struct {
	AnchorKey ed25519.PublicKey
	Signature []byte
}

// EndorsedRecord is what a node's latest fleet.peer record says about its
// endorsement: the identity key that signed the record, the record's IssuedAt
// (unix nanoseconds) and the endorsement it carries. AnchorEndorsements
// sources return one per peer.
type EndorsedRecord struct {
	NodeKey     ed25519.PublicKey
	IssuedAt    int64
	Endorsement AnchorEndorsement
}

// AnchorEndorsements answers, from the fleet.peer records a node holds, the
// endorsement each peer's latest record carries. node's RoleTable implements
// it; Policy.AuthorizeObserver reads it on every observer check, so a peer
// whose record is pruned or tombstoned stops counting at once.
type AnchorEndorsements interface {
	AnchorEndorsements() map[ports.NodeID]EndorsedRecord
}

// anchorEndorsementMessage is the byte string an endorsement signs. The node
// ID is length-prefixed so no ID/key split of the same bytes verifies.
// IssuedAt binds the endorsement to the record carrying it: a node identity
// key alone cannot republish a fresh record that still verifies, so a node
// whose anchor key is gone stops being an observer once node's RoleTable
// prunes its last endorsed record.
func anchorEndorsementMessage(id ports.NodeID, nodeKey ed25519.PublicKey, issuedAt int64) []byte {
	var b bytes.Buffer
	b.WriteString(anchorEndorsementDomain)
	var n [8]byte
	binary.BigEndian.PutUint16(n[:2], uint16(len(id)))
	b.Write(n[:2])
	b.WriteString(string(id))
	b.Write(nodeKey)
	binary.BigEndian.PutUint64(n[:], uint64(issuedAt))
	b.Write(n[:])
	return b.Bytes()
}

// SignAnchorEndorsement returns the tag value an anchor-key holder puts on its
// fleet.peer record (AnchorEndorsementTag): its anchor public key and its
// signature over the record's node ID, identity key and IssuedAt. node's
// PeerPublisher calls it on every publish of a holder's record.
func SignAnchorEndorsement(anchorKey ed25519.PrivateKey, id ports.NodeID, nodeKey ed25519.PublicKey, issuedAt int64) (string, error) {
	if len(anchorKey) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("anchor endorsement: anchor private key size %d", len(anchorKey))
	}
	if len(nodeKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("anchor endorsement: node key size %d", len(nodeKey))
	}
	if len(id) > 0xffff {
		return "", errors.New("anchor endorsement: node ID too long")
	}
	pub := anchorKey.Public().(ed25519.PublicKey)
	sig := ed25519.Sign(anchorKey, anchorEndorsementMessage(id, nodeKey, issuedAt))
	return hex.EncodeToString(pub) + "." + hex.EncodeToString(sig), nil
}

// ParseAnchorEndorsement decodes an AnchorEndorsementTag value. node's
// RoleTable calls it on each record it reports; a value that does not decode
// carries no endorsement.
func ParseAnchorEndorsement(v string) (AnchorEndorsement, error) {
	keyHex, sigHex, ok := strings.Cut(v, ".")
	if !ok {
		return AnchorEndorsement{}, errors.New("anchor endorsement: no key/signature separator")
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return AnchorEndorsement{}, errors.New("anchor endorsement: malformed anchor public key")
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return AnchorEndorsement{}, errors.New("anchor endorsement: malformed signature")
	}
	return AnchorEndorsement{AnchorKey: key, Signature: sig}, nil
}

// verify reports whether e is a valid signature by its own AnchorKey over
// (id, nodeKey, issuedAt). Whether that key is one the mesh lists is the
// caller's check (Policy.endorsedKey).
func (e AnchorEndorsement) verify(id ports.NodeID, nodeKey ed25519.PublicKey, issuedAt int64) bool {
	if len(e.AnchorKey) != ed25519.PublicKeySize || len(nodeKey) != ed25519.PublicKeySize || len(id) > 0xffff {
		return false
	}
	return ed25519.Verify(e.AnchorKey, anchorEndorsementMessage(id, nodeKey, issuedAt), e.Signature)
}

// verifyAnchorEndorsement is the one signature check Policy makes of a
// record's endorsement (AnchorEndorsement.verify). Policy.endorsementVerifies
// calls it at most once per record digest; a variable so the flood test in
// this package counts the checks made.
var verifyAnchorEndorsement = AnchorEndorsement.verify

// endorsementDigest identifies one record's endorsement claim: the node ID,
// its identity key, the record's IssuedAt, and the anchor key and signature
// the record carries, each length-prefixed so no two claims share a digest
// by splitting the same bytes differently. Two records with the same digest
// make the same claim, so one signature check's outcome stands for both;
// Policy.endorsementVerifies keys its cache by it, and a peer's crafted
// record therefore cannot plant a refusal under a valid record's digest.
func endorsementDigest(id ports.NodeID, rec EndorsedRecord) [sha256.Size]byte {
	h := sha256.New()
	var issued [8]byte
	binary.BigEndian.PutUint64(issued[:], uint64(rec.IssuedAt))
	for _, field := range [][]byte{[]byte(id), rec.NodeKey, issued[:], rec.Endorsement.AnchorKey, rec.Endorsement.Signature} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(field)))
		h.Write(n[:])
		h.Write(field)
	}
	var d [sha256.Size]byte
	h.Sum(d[:0])
	return d
}
