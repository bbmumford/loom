/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"errors"
	"fmt"

	"github.com/ORBTR/aether"
)

// The wire version entries this package owns. Every mesh session offers them
// in its aether handshake (node's meshWireVersions adds them to the Noise
// transports' and the byte-stream sessions' options), both required, so a peer
// that cannot speak the same version of each is refused before any stream
// exists. NegotiatedProfile reads the versions the session chose; the gossip
// loops run under that Profile.
const (
	// WireVersionCodec names the encoding of the G1 exchange: the
	// GossipEnvelope protobuf and the LADRecord records it carries.
	WireVersionCodec = "gossip.codec"
	// WireVersionReconcile names the reconciliation profile: the bucket
	// count and bounds of the GB bucket digest.
	WireVersionReconcile = "gossip.reconcile"
)

// CodecVersion is the gossip.codec version this build speaks: envelopes carry
// codec_version 1 and schema_version 1, and records are pb.LADRecord.
const CodecVersion uint16 = 1

// ReconcileVersion is the gossip.reconcile version this build speaks; its
// bucket count and bounds are reconcileProfiles[ReconcileVersion].
const ReconcileVersion uint16 = 1

// envelopeVersions maps a gossip.codec version to the codec_version and
// schema_version pair its envelopes carry. An envelope carrying any other pair
// is refused before a record decodes (checkEnvelopeVersions).
var envelopeVersions = map[uint16][2]uint32{
	1: {1, 1},
}

// reconcileProfiles maps a gossip.reconcile version to its profile. Version 1
// is the 64-bucket XOR digest in whisper's G2B encoding, with the bucket counts
// a requester may send bounded to [1, 256], the range the frame's count byte
// carries.
var reconcileProfiles = map[uint16]ReconcileProfile{
	1: {Version: 1, Buckets: 64, MinBuckets: 1, MaxBuckets: 256},
}

// WireVersions returns the entries this package adds to a mesh session's wire
// version offer: gossip.codec and gossip.reconcile, each at the one version
// this build speaks and each required.
func WireVersions() aether.WireVersions {
	return aether.WireVersions{
		{Name: WireVersionCodec, Versions: []uint16{CodecVersion}, Required: true},
		{Name: WireVersionReconcile, Versions: []uint16{ReconcileVersion}, Required: true},
	}
}

// ReconcileProfile is one gossip.reconcile version. Buckets is the count the
// initiator's GB request sends; a responder digests at whatever count the
// request carries when it lies in [MinBuckets, MaxBuckets] and refuses any
// other (whisper.WithDigestBucketBounds), so the two sides always compare one
// bucket partition.
type ReconcileProfile struct {
	Version    uint16
	Buckets    uint16
	MinBuckets uint16
	MaxBuckets uint16
}

// Profile is what a session negotiated for gossip: the codec its envelopes
// are written and read in, and its reconciliation profile.
// RunGossipLoopWithPeer and RunGossipResponder run under it.
type Profile struct {
	Codec     uint16
	Reconcile ReconcileProfile
}

// envelope returns the codec_version and schema_version pair the profile's
// envelopes carry.
func (p Profile) envelope() (codec, schema uint32) {
	v := envelopeVersions[p.Codec]
	return v[0], v[1]
}

// ErrWireProfile is the refusal NegotiatedProfile returns for a session whose
// chosen set lacks one of this package's entries or names a version this
// build does not speak. errors.Is matches it on a *WireProfileError.
var ErrWireProfile = errors.New("gossip: session negotiated no supported gossip profile")

// WireProfileError names the entry NegotiatedProfile refused on. Chosen is
// false when the session chose no version of it.
type WireProfileError struct {
	Name    string
	Version uint16
	Chosen  bool
}

func (e *WireProfileError) Error() string {
	if !e.Chosen {
		return fmt.Sprintf("%v: %s not chosen", ErrWireProfile, e.Name)
	}
	return fmt.Sprintf("%v: %s version %d not supported", ErrWireProfile, e.Name, e.Version)
}

// Is lets errors.Is(err, ErrWireProfile) match.
func (e *WireProfileError) Is(target error) bool { return target == ErrWireProfile }

// NegotiatedProfile returns the Profile a session's chosen wire version set
// selects. A set without gossip.codec or gossip.reconcile, or with a version
// this build does not speak, is refused with a *WireProfileError. node's
// AcceptMeshConnection calls it before it records any state for the session,
// and refuses the session on an error.
func NegotiatedProfile(set aether.NegotiatedWireVersions) (Profile, error) {
	codec, ok := set.Version(WireVersionCodec)
	if !ok {
		return Profile{}, &WireProfileError{Name: WireVersionCodec}
	}
	if _, ok := envelopeVersions[codec]; !ok {
		return Profile{}, &WireProfileError{Name: WireVersionCodec, Version: codec, Chosen: true}
	}
	rv, ok := set.Version(WireVersionReconcile)
	if !ok {
		return Profile{}, &WireProfileError{Name: WireVersionReconcile}
	}
	rp, ok := reconcileProfiles[rv]
	if !ok {
		return Profile{}, &WireProfileError{Name: WireVersionReconcile, Version: rv, Chosen: true}
	}
	return Profile{Codec: codec, Reconcile: rp}, nil
}
