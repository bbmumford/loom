/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/ORBTR/aether"
)

// The join proof of a seed's /mesh/vl1 upgrade.
//
// A seed session is a byte stream under the seed's TLS certificate: TLS
// authenticates the seed's host name and nothing about either node, and no
// Noise handshake runs on it, so no network key keys it. Without a proof the
// session would bind no partition (transportPartitionOf answers "" for it), and
// bindAuthenticatedPrincipal refuses every principal over a session bound to
// none; and a node holding any network key, or none, would join the mesh
// through the seed while the anchor verifier is unreachable, because
// handleVL1Upgrade lets the join proceed when that verifier answers nothing.
//
// The proof is a challenge and answer over two fresh nonces, one from each
// side, carried in the upgrade request, the 101 answer and the first bytes of
// the upgraded stream:
//
//  1. The joiner's request names the partition it joins under and its nonce
//     (seedJoinPartitionHeader, seedJoinNonceHeader) beside X-VL1-Node-ID.
//  2. The seed refuses, before the upgrade, a request carrying no proof
//     headers or naming a partition it holds no network key for. Otherwise
//     its 101 answer carries its own nonce and its proof over the transcript
//     (seedJoinTranscript): its Ed25519 identity key, a signature by that key
//     and an HMAC under its active network key for that partition.
//  3. The joiner checks the seed's proof against its own keys for the
//     partition and the seed's stated node ID, then writes its own proof
//     (seedJoinProofSize bytes: key, signature, HMAC) as the stream's first
//     bytes.
//  4. The seed checks the joiner's proof against its keys and the joiner's
//     stated node ID, and answers one byte, seedJoinAccepted, or closes the
//     stream. Nothing about the joiner is recorded before this check passes.
//
// A proof is accepted when its HMAC verifies under any key the checking side
// accepts for the partition (the active key first, then the keys accepted for
// receiving), its key derives the stated node ID (aether.NewNodeID), and its
// signature verifies over the transcript. Both nonces are in both transcripts,
// so no proof answers another session's challenge, and the two roles sign
// different transcripts, so the seed's answer to a challenge it is sent never
// serves as a joiner's proof.
//
// The accepted stream is bound to the partition both proofs name
// (seedJoinBoundConn.ScopeID), so AcceptIncomingMeshSession and
// DialAndAcceptMesh register the session under it, as a noise session's
// handshake binds one through its preamble.
const (
	seedJoinPartitionHeader = "X-VL1-Join-Partition"
	seedJoinNonceHeader     = "X-VL1-Join-Nonce"
	seedJoinKeyHeader       = "X-VL1-Join-Key"
	seedJoinSignatureHeader = "X-VL1-Join-Signature"
	seedJoinMACHeader       = "X-VL1-Join-MAC"

	seedJoinNonceSize = 32
	seedJoinProofSize = ed25519.PublicKeySize + ed25519.SignatureSize + sha256.Size
	seedJoinAccepted  = byte(1)

	// seedJoinProofTimeout bounds each side's wait for the other's proof or
	// answer on the upgraded stream, so a peer that stops after the 101 holds
	// no hijacked connection open.
	seedJoinProofTimeout = 10 * time.Second

	seedJoinRoleSeed   = "seed"
	seedJoinRoleJoiner = "joiner"
)

// ErrSeedJoinRefused is the error JoinSeed and the seed dial wrap when a join
// proof fails on either side: the seed's proof does not verify under this
// node's keys for the partition, or the seed closed the stream on this node's
// proof. errors.Is reads it.
var ErrSeedJoinRefused = errors.New("seed join refused")

// seedJoinTranscript is the byte string both proofs of one join sign and MAC:
// a fixed label, the role making the proof, the partition, both node IDs and
// both nonces, each field length-prefixed so no two field lists encode alike.
func seedJoinTranscript(role, partition, joinerID, seedID string, joinerNonce, seedNonce []byte) []byte {
	fields := [][]byte{
		[]byte("loom.vl1.seed-join.v1"), []byte(role), []byte(partition),
		[]byte(joinerID), []byte(seedID), joinerNonce, seedNonce,
	}
	var out []byte
	for _, f := range fields {
		out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
		out = append(out, f...)
	}
	return out
}

// seedJoinPartition is the partition this node names when it joins through a
// seed: the one partition its shared transport holds, the partition its own
// fleet.peer record names (advertisedDialPartition), or "" when it holds
// none or several, which joins under the unpartitioned keys (cfg.NetworkKeys).
func (rt *Runtime) seedJoinPartition() string {
	if partition, ok := rt.advertisedDialPartition(); ok {
		return partition
	}
	return ""
}

// seedJoinKeys returns the network keys this node accepts for partition on a
// seed join, the active key first: for "" the unpartitioned keys
// (cfg.NetworkKeys, which keep the shared transport's preamble-less sessions
// too); otherwise the shared transport's current keys for it, read at each
// join so UpdatePartitionKeys takes effect on the next one, or a dedicated
// partition's configured keys. nil means this node holds no key for it, and
// both sides then refuse the join.
func (rt *Runtime) seedJoinKeys(partition string) [][]byte {
	toBytes := func(keys []string) [][]byte {
		out := make([][]byte, 0, len(keys))
		for _, k := range keys {
			if k != "" {
				out = append(out, []byte(k))
			}
		}
		return out
	}
	if partition == "" {
		return toBytes(rt.cfg.NetworkKeys)
	}
	if rt.sharedTransport != nil {
		if keys, err := rt.sharedTransport.ResolveKeys(partition); err == nil && len(keys) > 0 {
			return keys
		}
	}
	for _, tc := range rt.cfg.Partitions {
		if tc.Dedicated && tc.PartitionID == partition {
			return toBytes(tc.NetworkKeys)
		}
	}
	return nil
}

// seedJoinProof is one side's proof: its identity key, its signature over the
// transcript and the transcript's HMAC under its active network key.
type seedJoinProof struct {
	key ed25519.PublicKey
	sig []byte
	mac []byte
}

func (rt *Runtime) makeSeedJoinProof(transcript []byte, activeKey []byte) seedJoinProof {
	m := hmac.New(sha256.New, activeKey)
	m.Write(transcript)
	return seedJoinProof{
		key: rt.identity.PublicKey,
		sig: ed25519.Sign(rt.identity.PrivateKey, transcript),
		mac: m.Sum(nil),
	}
}

// verify checks p as the proof of the node stated as nodeID: the key derives
// nodeID, the signature verifies over transcript, and the HMAC verifies under
// one of keys.
func (p seedJoinProof) verify(transcript []byte, nodeID string, keys [][]byte) error {
	if len(p.key) != ed25519.PublicKeySize || len(p.sig) != ed25519.SignatureSize || len(p.mac) != sha256.Size {
		return errors.New("malformed join proof")
	}
	derived, err := aether.NewNodeID(p.key)
	if err != nil || nodeID == "" || aether.NormalizeNodeID(string(derived)) != aether.NormalizeNodeID(nodeID) {
		return fmt.Errorf("the join proof's key does not derive node %s", truncID(nodeID))
	}
	if !ed25519.Verify(p.key, transcript, p.sig) {
		return errors.New("the join proof's signature does not verify")
	}
	for _, k := range keys {
		m := hmac.New(sha256.New, k)
		m.Write(transcript)
		if hmac.Equal(m.Sum(nil), p.mac) {
			return nil
		}
	}
	return errors.New("the join proof is not made under a network key this node holds for the partition")
}

func (p seedJoinProof) bytes() []byte {
	out := make([]byte, 0, seedJoinProofSize)
	out = append(out, p.key...)
	out = append(out, p.sig...)
	return append(out, p.mac...)
}

func seedJoinProofFrom(b []byte) seedJoinProof {
	return seedJoinProof{
		key: ed25519.PublicKey(b[:ed25519.PublicKeySize]),
		sig: b[ed25519.PublicKeySize : ed25519.PublicKeySize+ed25519.SignatureSize],
		mac: b[ed25519.PublicKeySize+ed25519.SignatureSize:],
	}
}

func seedJoinNonce() ([]byte, error) {
	n := make([]byte, seedJoinNonceSize)
	if _, err := rand.Read(n); err != nil {
		return nil, err
	}
	return n, nil
}

func decodeSeedJoinField(v string, size int) ([]byte, bool) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil || len(b) != size {
		return nil, false
	}
	return b, true
}

var seedJoinB64 = base64.RawURLEncoding.EncodeToString

// seedJoinBoundConn is an upgraded seed stream whose join proofs passed. It
// reads through r when set, the bufio.Reader that already holds whatever the
// peer wrote past the bytes the join read (the hijacked request's reader on
// the seed, the 101 answer's body on the joiner), and names the partition both
// proofs were made for, which transportPartitionOf reads through
// aether.PartitionAwareSession.
type seedJoinBoundConn struct {
	net.Conn
	r         io.Reader
	partition string
}

func (c *seedJoinBoundConn) Read(b []byte) (int, error) {
	if c.r != nil {
		return c.r.Read(b)
	}
	return c.Conn.Read(b)
}

// ScopeID is the partition the join proved; "" for a join under the
// unpartitioned keys.
func (c *seedJoinBoundConn) ScopeID() string { return c.partition }

// seedJoinChallenge is the seed's half of one join, built from the upgrade
// request before the hijack.
type seedJoinChallenge struct {
	partition   string
	keys        [][]byte
	joinerID    string
	joinerNonce []byte
	seedNonce   []byte
	proof       seedJoinProof
}

// openSeedJoin reads the joiner's proof headers from r and answers the seed's
// challenge, or the HTTP status and reason handleVL1Upgrade refuses the
// upgrade with: a request with no node ID or nonce, or naming a partition this
// node holds no network key for, is refused here, before the anchor verifier is
// asked and before anything about the joiner is recorded.
func (rt *Runtime) openSeedJoin(r *http.Request, joinerID string) (*seedJoinChallenge, int, error) {
	if joinerID == "" {
		return nil, http.StatusForbidden, errors.New("a seed join names its node ID (X-VL1-Node-ID)")
	}
	joinerNonce, ok := decodeSeedJoinField(r.Header.Get(seedJoinNonceHeader), seedJoinNonceSize)
	if !ok {
		return nil, http.StatusForbidden, errors.New("a seed join proves the mesh's network key: no join nonce was sent")
	}
	partition := r.Header.Get(seedJoinPartitionHeader)
	keys := rt.seedJoinKeys(partition)
	if len(keys) == 0 {
		return nil, http.StatusForbidden, fmt.Errorf("this node holds no network key for partition %q", partition)
	}
	seedNonce, err := seedJoinNonce()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	seedID := string(rt.identity.NodeID)
	return &seedJoinChallenge{
		partition:   partition,
		keys:        keys,
		joinerID:    joinerID,
		joinerNonce: joinerNonce,
		seedNonce:   seedNonce,
		proof: rt.makeSeedJoinProof(
			seedJoinTranscript(seedJoinRoleSeed, partition, joinerID, seedID, joinerNonce, seedNonce), keys[0]),
	}, 0, nil
}

// writeHeaders writes the seed's nonce and proof into the 101 answer.
func (c *seedJoinChallenge) writeHeaders(rw *bufio.ReadWriter) {
	_, _ = rw.WriteString(seedJoinNonceHeader + ": " + seedJoinB64(c.seedNonce) + "\r\n")
	_, _ = rw.WriteString(seedJoinKeyHeader + ": " + seedJoinB64(c.proof.key) + "\r\n")
	_, _ = rw.WriteString(seedJoinSignatureHeader + ": " + seedJoinB64(c.proof.sig) + "\r\n")
	_, _ = rw.WriteString(seedJoinMACHeader + ": " + seedJoinB64(c.proof.mac) + "\r\n")
}

// acceptJoiner reads the joiner's proof from the hijacked stream (through
// reader, which may already hold it), checks it, and on success answers
// seedJoinAccepted and returns the stream bound to the partition. On failure
// the caller closes conn and records nothing about the joiner.
func (rt *Runtime) acceptJoiner(c *seedJoinChallenge, conn net.Conn, reader *bufio.Reader) (net.Conn, error) {
	_ = conn.SetReadDeadline(time.Now().Add(seedJoinProofTimeout))
	buf := make([]byte, seedJoinProofSize)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, fmt.Errorf("no join proof arrived: %w", err)
	}
	transcript := seedJoinTranscript(seedJoinRoleJoiner, c.partition, c.joinerID, string(rt.identity.NodeID),
		c.joinerNonce, c.seedNonce)
	if err := seedJoinProofFrom(buf).verify(transcript, c.joinerID, c.keys); err != nil {
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(seedJoinProofTimeout))
	if _, err := conn.Write([]byte{seedJoinAccepted}); err != nil {
		return nil, fmt.Errorf("answer the join: %w", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return &seedJoinBoundConn{Conn: conn, r: reader, partition: c.partition}, nil
}

// SeedJoin is a seed join that both proofs passed: Conn is the upgraded stream,
// bound to the partition the proofs name, for DialAndAcceptMesh as the
// initiator with proto ProtoTLS; NodeID, ServiceName and Region are what the
// seed's 101 answer states, NodeID proven by its join proof.
type SeedJoin struct {
	Conn        net.Conn
	NodeID      string
	ServiceName string
	Region      string
}

// JoinSeed dials host's /mesh/vl1, verifying its certificate against roots (nil:
// the system roots, which is what every seed dial of this runtime uses), and
// runs the join proof, through dialSeed, the dial dialBootstrapHost makes for
// every seed dial this package makes. Consumer: a composition root's test whose
// seed serves a certificate the system roots do not hold (Go's darwin verifier
// reads no certificate file a test can supply), which hands Conn to
// DialAndAcceptMesh as loom's own TLS dial does. A failed proof on either side
// returns an error wrapping ErrSeedJoinRefused with the stream closed.
func (rt *Runtime) JoinSeed(ctx context.Context, host string, roots *x509.CertPool) (SeedJoin, error) {
	res, err := rt.dialSeed(ctx, host, roots)
	if err != nil {
		return SeedJoin{}, err
	}
	return SeedJoin{
		Conn:        res.conn,
		NodeID:      res.nodeID,
		ServiceName: res.resp.Header.Get("X-VL1-Service-Name"),
		Region:      res.resp.Header.Get("X-VL1-Region"),
	}, nil
}

// finishSeedJoin is the joiner's half after the 101 answer: it checks the
// seed's proof from resp's headers against keys and seedID, writes this node's
// proof on conn, and waits for the seed's answer, reading through conn (whose
// reads go through resp.Body). It returns conn bound to partition.
func (rt *Runtime) finishSeedJoin(conn net.Conn, resp *http.Response, seedID, partition string,
	keys [][]byte, joinerNonce []byte) (net.Conn, error) {

	seedNonce, ok1 := decodeSeedJoinField(resp.Header.Get(seedJoinNonceHeader), seedJoinNonceSize)
	key, ok2 := decodeSeedJoinField(resp.Header.Get(seedJoinKeyHeader), ed25519.PublicKeySize)
	sig, ok3 := decodeSeedJoinField(resp.Header.Get(seedJoinSignatureHeader), ed25519.SignatureSize)
	mac, ok4 := decodeSeedJoinField(resp.Header.Get(seedJoinMACHeader), sha256.Size)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, fmt.Errorf("%w: the seed's 101 answer carries no join proof", ErrSeedJoinRefused)
	}
	joinerID := string(rt.identity.NodeID)
	seedTranscript := seedJoinTranscript(seedJoinRoleSeed, partition, joinerID, seedID, joinerNonce, seedNonce)
	if err := (seedJoinProof{key: key, sig: sig, mac: mac}).verify(seedTranscript, seedID, keys); err != nil {
		return nil, fmt.Errorf("%w: the seed's proof: %v", ErrSeedJoinRefused, err)
	}
	proof := rt.makeSeedJoinProof(
		seedJoinTranscript(seedJoinRoleJoiner, partition, joinerID, seedID, joinerNonce, seedNonce), keys[0])
	_ = conn.SetDeadline(time.Now().Add(seedJoinProofTimeout))
	if _, err := conn.Write(proof.bytes()); err != nil {
		return nil, fmt.Errorf("%w: write the join proof: %v", ErrSeedJoinRefused, err)
	}
	answer := make([]byte, 1)
	if _, err := io.ReadFull(conn, answer); err != nil || answer[0] != seedJoinAccepted {
		return nil, fmt.Errorf("%w: the seed did not accept this node's join proof", ErrSeedJoinRefused)
	}
	_ = conn.SetDeadline(time.Time{})
	return &seedJoinBoundConn{Conn: conn, partition: partition}, nil
}
