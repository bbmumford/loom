/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// The deterministic accepted-set digest shared by every ports.LiveDirectory
// implementation in this package. It lives on its own so both the Swarm
// projection and the transitional LAD adapter hash records through ONE
// function -- two directories can only be compared if they agree byte-for-byte
// on how a record is digested.
package directory

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/bbmumford/loom/ports"
)

// fingerprintRecords hashes sorted (topic, nodeID, [key,] hlc, sha256(sig))
// tuples — the same shape as the swarm Merkle leaf, so two directories with
// the same accepted set agree byte-for-byte.
//
// The key contribution is CONDITIONAL, emitted only when Key != "", for the
// same reason the signable-byte suffix is (swarm sig.go): every record in the
// fleet today is keyless, and their fingerprint must not move. A digest that
// changed shape for existing records would make every live directory disagree
// with every stored anchor input at once, on a change that added no
// information.
//
// Omitting the key entirely was a real defect, not a cosmetic one: slot
// identity is (Topic, NodeID, Key), so two directories holding a node's
// "blob-alpha" and its "blob-beta" are otherwise identical in every hashed
// field. The shadow gate (§0.5.3 stage 3) would have reported PARITY on
// divergent state — the one outcome that phase exists to prevent. Guarded by
// TestFingerprintDistinguishesDirectoriesDifferingOnlyByKey, with
// TestKeylessFingerprintIsTheLegacyTuple pinning the unkeyed bytes against an
// independently written pre-composite-key oracle.
func fingerprintRecords(recs []ports.Record) [32]byte {
	h := sha256.New()
	for _, r := range recs {
		th := sha256.Sum256([]byte(r.Topic))
		h.Write(th[:])
		nh := sha256.Sum256([]byte(r.NodeID))
		h.Write(nh[:])
		if r.Key != "" {
			var klen [2]byte
			binary.BigEndian.PutUint16(klen[:], uint16(len(r.Key)))
			h.Write(klen[:])
			kh := sha256.Sum256([]byte(r.Key))
			h.Write(kh[:])
		}
		var hlcb [8]byte
		binary.BigEndian.PutUint64(hlcb[:], uint64(r.HLC))
		h.Write(hlcb[:])
		sh := sha256.Sum256(r.Signature)
		h.Write(sh[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
