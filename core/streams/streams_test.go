/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package streams

import (
	"errors"
	"testing"

	"github.com/ORBTR/aether"
)

// The IDs are wire values both ends of a session must agree on, so each is
// pinned to its number rather than only to being distinct.
func TestStreamIDsAreFixedWireValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  uint64
		want uint64
	}{
		{"StreamGossip", StreamGossip, 0},
		{"StreamRPC", StreamRPC, 1},
		{"StreamKeepalive", StreamKeepalive, 2},
		{"StreamControl", StreamControl, 3},
		{"StreamReconcile", StreamReconcile, 4},
		{"DynamicRPCBase", DynamicRPCBase, 10},
		{"ApplicationBand", ApplicationBand, 100},
		{"StreamSwarm", StreamSwarm, 100},
		{"StreamMeshDB", StreamMeshDB, 101},
		{"StreamTunnelBase", StreamTunnelBase, 200},
		{"StreamTunnelLast", StreamTunnelLast, 299},
		{"StreamFileTransferBase", StreamFileTransferBase, 300},
		{"StreamFileTransferLast", StreamFileTransferLast, 399},
		{"StreamTelemetry", StreamTelemetry, 400},
		{"StreamPolicyPush", StreamPolicyPush, 500},
		{"StreamPolicyAck", StreamPolicyAck, 501},
		{"StreamDynamicBase", StreamDynamicBase, 600},
		{"StreamDynamicLast", StreamDynamicLast, 699},
		{"StreamRemoteTerminal", StreamRemoteTerminal, 600},
		{"StreamRemoteFilesData", StreamRemoteFilesData, 601},
		{"StreamRemoteFilesControl", StreamRemoteFilesControl, 602},
		{"StreamRemoteScreenVideo", StreamRemoteScreenVideo, 603},
		{"StreamRemoteScreenInput", StreamRemoteScreenInput, 604},
		{"StreamRemoteScreenControl", StreamRemoteScreenControl, 605},
		{"StreamRemoteWeb", StreamRemoteWeb, 606},
		{"RemoteAccessDynamicFloor", RemoteAccessDynamicFloor, 610},
		{"StreamPlatformConversation", StreamPlatformConversation, 700},
		{"StreamPlatformCoordination", StreamPlatformCoordination, 701},
		{"StreamPlatformDispatch", StreamPlatformDispatch, 702},
		{"StreamHandlerBase", StreamHandlerBase, 1000},
		{"StreamHandlerLast", StreamHandlerLast, 1999},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// Every ID the registry declares is declared once: a second entry covering an
// ID would let two consumers open the same number, and aether hands the second
// OPEN the stream the first already holds.
func TestRegistryAssignsEveryIDOnce(t *testing.T) {
	assignedTo := map[uint64]string{}
	names := map[string]bool{}
	for _, e := range Registry() {
		if e.Name == "" {
			t.Errorf("entry [%d, %d] has no name", e.First, e.Last)
		}
		if names[e.Name] {
			t.Errorf("two entries are named %q", e.Name)
		}
		names[e.Name] = true
		if e.Last < e.First {
			t.Errorf("%s: last %d is below first %d", e.Name, e.Last, e.First)
		}
		if e.Latency == 0 {
			t.Errorf("%s names no latency class", e.Name)
		}
		if e.OpenedBy == "" {
			t.Errorf("%s names no opener", e.Name)
		}
		for id := e.First; id <= e.Last; id++ {
			if other, dup := assignedTo[id]; dup {
				t.Errorf("stream ID %d is assigned to both %s and %s", id, other, e.Name)
			}
			assignedTo[id] = e.Name
		}
	}
	if len(assignedTo) == 0 {
		t.Fatal("the registry is empty")
	}
}

// A remote-access channel is numbered inside the dynamic peer band, so the
// device's dynamic accept path admits it; each channel number is its own, so
// the two ends of a channel cannot bind different channels to one ID.
func TestRemoteAccessChannelsSitInsideTheDynamicBand(t *testing.T) {
	seen := map[uint64]string{}
	names := map[string]bool{}
	for _, ch := range RemoteAccessChannels() {
		if ch.ID < StreamDynamicBase || ch.ID > StreamDynamicLast {
			t.Errorf("channel %s = %d, outside the dynamic band [%d, %d]", ch.Name, ch.ID, StreamDynamicBase, StreamDynamicLast)
		}
		if ch.ID >= RemoteAccessDynamicFloor {
			t.Errorf("channel %s = %d, at or above the floor %d a session numbers unnamed channels from", ch.Name, ch.ID, RemoteAccessDynamicFloor)
		}
		if other, dup := seen[ch.ID]; dup {
			t.Errorf("channels %s and %s share ID %d", ch.Name, other, ch.ID)
		}
		seen[ch.ID] = ch.Name
		if names[ch.Name] {
			t.Errorf("two channels are named %q", ch.Name)
		}
		names[ch.Name] = true
		if ch.Latency == 0 {
			t.Errorf("channel %s names no latency class", ch.Name)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no remote-access channel is declared")
	}
}

// Every exported ID the package declares is covered by the registry: a
// constant outside it would be a stream number no table accounts for.
func TestEveryDeclaredConstantIsInTheRegistry(t *testing.T) {
	declared := map[string]uint64{
		"StreamGossip":               StreamGossip,
		"StreamRPC":                  StreamRPC,
		"StreamKeepalive":            StreamKeepalive,
		"StreamControl":              StreamControl,
		"StreamReconcile":            StreamReconcile,
		"DynamicRPCBase":             DynamicRPCBase,
		"StreamSwarm":                StreamSwarm,
		"StreamMeshDB":               StreamMeshDB,
		"StreamTunnelBase":           StreamTunnelBase,
		"StreamTunnelLast":           StreamTunnelLast,
		"StreamFileTransferBase":     StreamFileTransferBase,
		"StreamFileTransferLast":     StreamFileTransferLast,
		"StreamTelemetry":            StreamTelemetry,
		"StreamPolicyPush":           StreamPolicyPush,
		"StreamPolicyAck":            StreamPolicyAck,
		"StreamDynamicBase":          StreamDynamicBase,
		"StreamDynamicLast":          StreamDynamicLast,
		"StreamRemoteTerminal":       StreamRemoteTerminal,
		"StreamRemoteFilesData":      StreamRemoteFilesData,
		"StreamRemoteFilesControl":   StreamRemoteFilesControl,
		"StreamRemoteScreenVideo":    StreamRemoteScreenVideo,
		"StreamRemoteScreenInput":    StreamRemoteScreenInput,
		"StreamRemoteScreenControl":  StreamRemoteScreenControl,
		"StreamRemoteWeb":            StreamRemoteWeb,
		"StreamPlatformConversation": StreamPlatformConversation,
		"StreamPlatformCoordination": StreamPlatformCoordination,
		"StreamPlatformDispatch":     StreamPlatformDispatch,
	}
	for name, id := range declared {
		covered := false
		for _, e := range Registry() {
			if id >= e.First && id <= e.Last {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("%s = %d is in no registry entry", name, id)
		}
	}
}

// DefaultLatencyClass answers from the registry, and a channel's class wins
// over the band it sits in: the scheduler must put screen video ahead of the
// bulk peer traffic sharing that band.
func TestDefaultLatencyClassAnswersTheRegistry(t *testing.T) {
	for _, e := range Registry() {
		if e.First >= StreamDynamicBase && e.First <= StreamDynamicLast {
			continue // the named channels own their IDs inside this band
		}
		if got := DefaultLatencyClass(e.First); got != e.Latency {
			t.Errorf("DefaultLatencyClass(%d) = %v, want %v for %s", e.First, got, e.Latency, e.Name)
		}
	}
	for _, ch := range RemoteAccessChannels() {
		if got := DefaultLatencyClass(ch.ID); got != ch.Latency {
			t.Errorf("DefaultLatencyClass(%d) = %v, want %v for channel %s", ch.ID, got, ch.Latency, ch.Name)
		}
	}
}

// An ID the registry assigns to nothing gets the lowest class, so an
// unrecognised stream cannot be scheduled ahead of a declared one.
func TestAnUnassignedIDGetsTheBulkClass(t *testing.T) {
	for _, id := range []uint64{5, 102, 450, 2000} {
		for _, e := range Registry() {
			if id >= e.First && id <= e.Last {
				t.Fatalf("%d is assigned to %s; pick an ID the registry leaves free", id, e.Name)
			}
		}
		if got := DefaultLatencyClass(id); got != aether.ClassBULK {
			t.Errorf("DefaultLatencyClass(%d) = %v, want ClassBULK", id, got)
		}
	}
}

// Swarm gossip and Raft heartbeats must not queue behind bulk transfers.
func TestDefaultLatencyClassKeepsPeerStreamsInteractive(t *testing.T) {
	for name, id := range map[string]uint64{"StreamSwarm": StreamSwarm, "StreamMeshDB": StreamMeshDB} {
		if got := DefaultLatencyClass(id); got != aether.ClassINTERACTIVE {
			t.Errorf("DefaultLatencyClass(%s) = %v, want ClassINTERACTIVE", name, got)
		}
	}
}

// The version travels with the assignments: a peer reading one version and a
// peer reading another do not agree on what arrives on an ID. It is pinned to its
// number, so an assignment added to Registry and this pin move in one change.
func TestRegistryVersionIsDeclared(t *testing.T) {
	if RegistryVersion != 2 {
		t.Errorf("RegistryVersion = %d, want 2", RegistryVersion)
	}
}

// The worker opens every arm of its crossing, and the edge's workeraccept claims
// each by ID, so the dispatch arm names the same opener and class as the two feeds
// beside it: a second opener would land on the worker's stream, and a bulk class
// would queue a pushed dispatch behind screen and file traffic.
func TestTheWorkerOpensItsDispatchArm(t *testing.T) {
	for _, e := range Registry() {
		if e.First != StreamPlatformDispatch {
			continue
		}
		if e.Last != e.First || e.OpenedBy != "the worker" || e.Latency != aether.ClassINTERACTIVE {
			t.Errorf("the dispatch arm's entry is %+v, want one ID opened by the worker at the interactive class", e)
		}
		return
	}
	t.Fatalf("no registry entry starts at %d", StreamPlatformDispatch)
}

// The registry's version is an entry of every mesh session's wire version
// offer, required, so two peers reading different tables are refused before
// either opens a stream, and the refusal names the entry.
func TestRegistryVersionIsARequiredWireVersion(t *testing.T) {
	w := WireVersions()
	if len(w) != 1 || w[0].Name != WireVersionRegistry || !w[0].Required ||
		len(w[0].Versions) != 1 || w[0].Versions[0] != RegistryVersion {
		t.Fatalf("WireVersions() = %+v, want streams.registry at %d, required", w, RegistryVersion)
	}
	if err := w.Validate(); err != nil {
		t.Fatalf("entry is not canonical: %v", err)
	}
	if _, err := aether.ChooseWireVersions(w, WireVersions()); err != nil {
		t.Fatalf("two peers reading this table refuse each other: %v", err)
	}
	other := WireVersions()
	other[0].Versions = []uint16{RegistryVersion + 1}
	_, err := aether.ChooseWireVersions(w, other)
	var me *aether.WireVersionMismatchError
	if !errors.As(err, &me) || me.Name != WireVersionRegistry {
		t.Fatalf("a peer reading table %d: err = %v, want a mismatch naming %s", RegistryVersion+1, err, WireVersionRegistry)
	}
}

// Per-peer application streams sit in aether's application band (>= 100) and
// never share an ID with each other.
func TestApplicationStreamsAreDistinctAndInTheApplicationBand(t *testing.T) {
	const applicationBand = ApplicationBand
	seen := map[uint64]string{}
	for name, id := range map[string]uint64{
		"StreamSwarm":  StreamSwarm,
		"StreamMeshDB": StreamMeshDB,
	} {
		if id < applicationBand {
			t.Errorf("%s = %d, below the application band %d", name, id, applicationBand)
		}
		if other, dup := seen[id]; dup {
			t.Errorf("%s and %s share stream ID %d", name, other, id)
		}
		seen[id] = name
	}
}
