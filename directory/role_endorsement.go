/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package directory

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bbmumford/loom/ports"
)

// RoleEndorsementPeriods is how many republish periods a role endorsement is
// valid for: a node re-signs at boot and on every republish, so three periods
// survive two missed republishes, and an endorsement whose holder lost the role
// key lapses within three.
const RoleEndorsementPeriods = 3

// roleEndorsementDomain separates a role endorsement's signed bytes from every
// other message a role key signs.
const roleEndorsementDomain = "loom.role.endorsement.v1\x00"

// RoleKeys is what the signed mesh document says about the guarded roles: the
// public keys listed for each role, the republish period a holder re-signs on,
// and the document's own expiry. A role is revoked by removing its key from the
// document, which every endorsement under that key then fails.
type RoleKeys struct {
	// Keys lists, for each role, the role public keys the document carries.
	Keys map[string][]ed25519.PublicKey
	// RepublishPeriod is the period a holder re-signs its endorsement on; it
	// must be positive.
	RepublishPeriod time.Duration
	// DocumentNotAfter is the signed document's expiry. An endorsement is
	// never valid past it; zero means the document states none.
	DocumentNotAfter time.Time
}

// listed reports whether key is one of role's public keys.
func (k RoleKeys) listed(role string, key ed25519.PublicKey) bool {
	for _, have := range k.Keys[role] {
		if bytes.Equal(have, key) {
			return true
		}
	}
	return false
}

// RoleEndorsement is a guarded role's signature, made with the role private key
// an entitled endpoint holds, over the role, a node's ID, the identity key that
// node signs its records with, and the endorsement's validity. Consumers admit a
// role claim, a sealed role bundle, an unwrap or a key release only for a node
// whose endorsement verifies (VerifyRoleEndorsement).
type RoleEndorsement struct {
	Role     string
	NodeID   ports.NodeID
	NodeKey  ed25519.PublicKey
	IssuedAt int64 // unix nanoseconds
	NotAfter int64 // unix nanoseconds
	RoleKey  ed25519.PublicKey
	// Signature covers every field above except RoleKey, which identifies the
	// key that made it.
	Signature []byte
}

var (
	// ErrRoleKeyNotListed is an endorsement, or a signing, under a key the
	// document does not list for the role.
	ErrRoleKeyNotListed = errors.New("role endorsement: the role key is not listed for the role")
	// ErrRoleEndorsementSignature is a signature that does not verify.
	ErrRoleEndorsementSignature = errors.New("role endorsement: the signature does not verify")
	// ErrRoleEndorsementExpired is an endorsement at or past its NotAfter.
	ErrRoleEndorsementExpired = errors.New("role endorsement: expired")
	// ErrRoleEndorsementNotYetValid is an endorsement issued later than the
	// verifier's clock allows (MaxIssuedAtSkew).
	ErrRoleEndorsementNotYetValid = errors.New("role endorsement: issued in the future")
	// ErrRoleEndorsementSpan is a validity longer than RoleEndorsementPeriods
	// republish periods or past the document's expiry.
	ErrRoleEndorsementSpan = errors.New("role endorsement: valid for longer than the document allows")
)

// roleEndorsementMessage is the byte string an endorsement signs. Each string is
// length-prefixed so no split of the same bytes between role, node ID and key
// verifies.
func roleEndorsementMessage(role string, id ports.NodeID, nodeKey ed25519.PublicKey, issuedAt, notAfter int64) []byte {
	var b bytes.Buffer
	b.WriteString(roleEndorsementDomain)
	var n [8]byte
	for _, s := range []string{role, string(id)} {
		binary.BigEndian.PutUint16(n[:2], uint16(len(s)))
		b.Write(n[:2])
		b.WriteString(s)
	}
	b.Write(nodeKey)
	binary.BigEndian.PutUint64(n[:], uint64(issuedAt))
	b.Write(n[:])
	binary.BigEndian.PutUint64(n[:], uint64(notAfter))
	b.Write(n[:])
	return b.Bytes()
}

// maxNotAfter is the latest NotAfter keys allow for an endorsement issued at
// issuedAt.
func (k RoleKeys) maxNotAfter(issuedAt int64) int64 {
	limit := issuedAt + int64(RoleEndorsementPeriods)*int64(k.RepublishPeriod)
	if !k.DocumentNotAfter.IsZero() && k.DocumentNotAfter.UnixNano() < limit {
		limit = k.DocumentNotAfter.UnixNano()
	}
	return limit
}

// SignRoleEndorsement signs role's endorsement of the node id with identity key
// nodeKey at now, valid for RoleEndorsementPeriods republish periods and never
// past the document's expiry. It refuses a role key the document does not list
// for role, and a document already expired at now.
func SignRoleEndorsement(roleKey ed25519.PrivateKey, role string, id ports.NodeID, nodeKey ed25519.PublicKey, now time.Time, keys RoleKeys) (RoleEndorsement, error) {
	switch {
	case len(roleKey) != ed25519.PrivateKeySize:
		return RoleEndorsement{}, fmt.Errorf("role endorsement: role private key size %d", len(roleKey))
	case len(nodeKey) != ed25519.PublicKeySize:
		return RoleEndorsement{}, fmt.Errorf("role endorsement: node key size %d", len(nodeKey))
	case role == "" || len(role) > 0xffff || len(id) > 0xffff:
		return RoleEndorsement{}, errors.New("role endorsement: role or node ID empty or too long")
	case keys.RepublishPeriod <= 0:
		return RoleEndorsement{}, errors.New("role endorsement: the document states no republish period")
	}
	pub := roleKey.Public().(ed25519.PublicKey)
	if !keys.listed(role, pub) {
		return RoleEndorsement{}, fmt.Errorf("%w: %s", ErrRoleKeyNotListed, role)
	}
	issuedAt := now.UnixNano()
	notAfter := keys.maxNotAfter(issuedAt)
	if notAfter <= issuedAt {
		return RoleEndorsement{}, fmt.Errorf("%w: the document expired at %s", ErrRoleEndorsementSpan, keys.DocumentNotAfter.UTC().Format(time.RFC3339))
	}
	return RoleEndorsement{
		Role: role, NodeID: id, NodeKey: append(ed25519.PublicKey(nil), nodeKey...),
		IssuedAt: issuedAt, NotAfter: notAfter, RoleKey: pub,
		Signature: ed25519.Sign(roleKey, roleEndorsementMessage(role, id, nodeKey, issuedAt, notAfter)),
	}, nil
}

// VerifyRoleEndorsement reports whether e is a valid endorsement at now under
// keys: its role key is listed for its role, the signature verifies, it was not
// issued later than now plus MaxIssuedAtSkew, now is before NotAfter, and its
// validity is within RoleEndorsementPeriods republish periods and the
// document's expiry. It binds e to e.NodeID and e.NodeKey only; the caller
// checks those are the node it is admitting.
func VerifyRoleEndorsement(e RoleEndorsement, keys RoleKeys, now time.Time) error {
	if len(e.RoleKey) != ed25519.PublicKeySize || len(e.NodeKey) != ed25519.PublicKeySize || len(e.Signature) != ed25519.SignatureSize {
		return ErrRoleEndorsementSignature
	}
	if !keys.listed(e.Role, e.RoleKey) {
		return fmt.Errorf("%w: %s", ErrRoleKeyNotListed, e.Role)
	}
	if !ed25519.Verify(e.RoleKey, roleEndorsementMessage(e.Role, e.NodeID, e.NodeKey, e.IssuedAt, e.NotAfter), e.Signature) {
		return ErrRoleEndorsementSignature
	}
	if keys.RepublishPeriod <= 0 || e.NotAfter <= e.IssuedAt || e.NotAfter > keys.maxNotAfter(e.IssuedAt) {
		return ErrRoleEndorsementSpan
	}
	if e.IssuedAt > now.Add(MaxIssuedAtSkew).UnixNano() {
		return ErrRoleEndorsementNotYetValid
	}
	if now.UnixNano() >= e.NotAfter {
		return ErrRoleEndorsementExpired
	}
	return nil
}

// Encode returns e as one tag value: role, node ID, node key, IssuedAt,
// NotAfter, role key and signature, separated by ".", the strings hex-encoded
// so no separator appears inside one.
func (e RoleEndorsement) Encode() string {
	return strings.Join([]string{
		hex.EncodeToString([]byte(e.Role)), hex.EncodeToString([]byte(e.NodeID)), hex.EncodeToString(e.NodeKey),
		strconv.FormatInt(e.IssuedAt, 10), strconv.FormatInt(e.NotAfter, 10),
		hex.EncodeToString(e.RoleKey), hex.EncodeToString(e.Signature),
	}, ".")
}

// ParseRoleEndorsement decodes an Encode value. A value that does not decode
// carries no endorsement; decoding verifies nothing (VerifyRoleEndorsement).
func ParseRoleEndorsement(v string) (RoleEndorsement, error) {
	parts := strings.Split(v, ".")
	if len(parts) != 7 {
		return RoleEndorsement{}, errors.New("role endorsement: expected seven fields")
	}
	var e RoleEndorsement
	role, err1 := hex.DecodeString(parts[0])
	id, err2 := hex.DecodeString(parts[1])
	nodeKey, err3 := hex.DecodeString(parts[2])
	issuedAt, err4 := strconv.ParseInt(parts[3], 10, 64)
	notAfter, err5 := strconv.ParseInt(parts[4], 10, 64)
	roleKey, err6 := hex.DecodeString(parts[5])
	sig, err7 := hex.DecodeString(parts[6])
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7); err != nil {
		return RoleEndorsement{}, fmt.Errorf("role endorsement: malformed field: %w", err)
	}
	if len(nodeKey) != ed25519.PublicKeySize || len(roleKey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return RoleEndorsement{}, errors.New("role endorsement: malformed key or signature")
	}
	e.Role, e.NodeID, e.NodeKey, e.IssuedAt, e.NotAfter, e.RoleKey, e.Signature =
		string(role), ports.NodeID(id), nodeKey, issuedAt, notAfter, roleKey, sig
	return e, nil
}

// roleEndorsementTagPrefix opens the Capabilities.tags key a node's fleet.peer
// record carries one role endorsement under.
const roleEndorsementTagPrefix = "role_endorsement."

// RoleEndorsementTag is the Capabilities.tags key under which a node's
// fleet.peer record carries its current endorsement of role, as
// "role_endorsement.<role>=<Encode value>". node's PeerPublisher writes one per
// role the node holds an endorsement of, and node's RoleTable reads it back so
// a host judging this node as a caller verifies the endorsement under its own
// document's RoleKeys. It rides a tag for the reason AnchorEndorsementTag does:
// swarm's PeerRecord has no endorsement field.
func RoleEndorsementTag(role string) string { return roleEndorsementTagPrefix + role }
