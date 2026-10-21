/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/noise"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/core/streams"
)

// loomEntryNames are the application entries every mesh session offers.
var loomEntryNames = []string{gossip.WireVersionCodec, gossip.WireVersionReconcile, streams.WireVersionRegistry}

// loomNegotiatedSet is the set two peers of this build choose.
func loomNegotiatedSet() aether.NegotiatedWireVersions {
	own, err := aether.LocalWireVersions(meshWireVersions(nil))
	if err != nil {
		panic(err)
	}
	set, err := aether.ChooseWireVersions(own, own)
	if err != nil {
		panic(err)
	}
	return set
}

// probeSession reports the set two peers of this build choose, as a session
// built by aether's handshake between them does, so AcceptMeshConnection
// admits it.
func (s *probeSession) WireVersions() aether.NegotiatedWireVersions { return loomNegotiatedSet() }

// withLoomEntryAt returns loom's entries with name moved to version v, the
// offer of a peer whose build reads another table or codec.
func withLoomEntryAt(t *testing.T, name string, v uint16) aether.WireVersions {
	t.Helper()
	out := meshWireVersions(nil)
	found := false
	for i := range out {
		if out[i].Name == name {
			out[i].Versions = []uint16{v}
			found = true
		}
	}
	if !found {
		t.Fatalf("loom offers no %s", name)
	}
	return out
}

// Every mesh session offers loom's three entries, each required, and a
// caller's own entries are kept beside them, except one that uses a loom name,
// which loom's replaces.
func TestMeshWireVersionsCarryLoomsRequiredEntries(t *testing.T) {
	got := meshWireVersions(nil)
	for _, name := range loomEntryNames {
		found := false
		for _, e := range got {
			if e.Name == name {
				found = true
				if !e.Required {
					t.Errorf("%s is optional", name)
				}
			}
		}
		if !found {
			t.Errorf("mesh sessions do not offer %s", name)
		}
	}
	if _, err := aether.LocalWireVersions(got); err != nil {
		t.Fatalf("the entries are not a valid offer: %v", err)
	}
	set := loomNegotiatedSet()
	if v, _ := set.Version(streams.WireVersionRegistry); v != streams.RegistryVersion {
		t.Errorf("streams.registry chosen at %d, want RegistryVersion %d", v, streams.RegistryVersion)
	}

	app := aether.WireVersions{
		{Name: "app.extra", Versions: []uint16{3}},
		{Name: gossip.WireVersionCodec, Versions: []uint16{9}, Required: false},
	}
	merged := meshWireVersions(app)
	var extra, codec int
	for _, e := range merged {
		switch e.Name {
		case "app.extra":
			extra++
		case gossip.WireVersionCodec:
			codec++
			if len(e.Versions) != 1 || e.Versions[0] != gossip.CodecVersion || !e.Required {
				t.Errorf("caller's gossip.codec %v survived in place of loom's", e)
			}
		}
	}
	if extra != 1 || codec != 1 {
		t.Fatalf("merged offer %v: app.extra %d times, gossip.codec %d times; want once each", merged, extra, codec)
	}
}

// wireTestRuntime is the part of a Runtime SetupMeshSession reads.
func wireTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	id, err := identityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return &Runtime{identity: id, sessionOpts: aether.DefaultSessionOptions()}
}

// A byte-stream mesh session that SetupMeshSession builds offers loom's
// entries even when the runtime's session options carry none, so a peer whose
// streams.registry, gossip.codec or gossip.reconcile differs is refused by
// both ends before either opens a stream, and the refusal names the entry. A
// peer of this build is admitted with every entry chosen.
func TestByteStreamMeshSessionsRefuseAPeerWhoseLoomEntryDiffers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pair := func(t *testing.T, peerOffer aether.WireVersions) (aether.Session, aether.Session) {
		t.Helper()
		rt := wireTestRuntime(t)
		other := wireTestRuntime(t)
		near, far := net.Pipe()
		sess, err := rt.SetupMeshSession(ctx, near, string(other.identity.NodeID), ProtoWebSocket, true)
		if err != nil {
			t.Fatalf("SetupMeshSession: %v", err)
		}
		opts := aether.DefaultSessionOptions()
		opts.WireVersions = peerOffer
		peer := adapter.NewTCPSession(far, other.identity.NodeID, rt.identity.NodeID, aether.ProtoTCP, opts)
		t.Cleanup(func() { _ = sess.Close(); _ = peer.Close() })
		return sess, peer
	}

	for _, name := range loomEntryNames {
		t.Run(name, func(t *testing.T) {
			sess, peer := pair(t, withLoomEntryAt(t, name, 2))
			accepted := make(chan error, 1)
			go func() {
				st, err := peer.AcceptStreamByID(ctx, streams.StreamGossip)
				if st != nil {
					err = errors.New("the peer accepted the gossip stream")
				}
				accepted <- err
			}()
			st, err := sess.OpenStream(ctx, aether.StreamConfig{StreamID: streams.StreamGossip})
			if st != nil {
				t.Fatal("the gossip stream opened on a session whose peer differs in " + name)
			}
			var me *aether.WireVersionMismatchError
			if !errors.As(err, &me) || me.Name != name {
				t.Fatalf("OpenStream err = %v, want a mismatch naming %s", err, name)
			}
			if err := <-accepted; !errors.As(err, &me) || me.Name != name {
				t.Fatalf("the peer's AcceptStreamByID err = %v, want a mismatch naming %s", err, name)
			}
		})
	}

	sess, peer := pair(t, meshWireVersions(nil))
	go func() { _, _ = peer.AcceptStreamByID(ctx, streams.StreamGossip) }()
	if _, err := sess.OpenStream(ctx, aether.StreamConfig{StreamID: streams.StreamGossip}); err != nil {
		t.Fatalf("a peer of this build: OpenStream: %v", err)
	}
	set, ok := aether.SessionWireVersions(sess)
	if !ok || !set.Equal(loomNegotiatedSet()) {
		t.Fatalf("a peer of this build: chosen set %v, want %v", set, loomNegotiatedSet())
	}
}

// reportingSession is a probeSession reporting a chosen set of the test's
// choosing.
type reportingSession struct {
	*probeSession
	set aether.NegotiatedWireVersions
}

func (s reportingSession) WireVersions() aether.NegotiatedWireVersions { return s.set }

// silentSession is a session that reports no chosen set at all.
type silentSession struct{ aether.Session }

// AcceptMeshConnection refuses a session that negotiated no gossip profile
// before it records anything for the peer: the connection table, the budget
// and the dispatch table are as they were, the session is closed, and the
// typed refusal is returned.
func TestAcceptMeshConnectionRefusesASessionWithoutLoomsEntries(t *testing.T) {
	ctx := context.Background()
	noCodec, err := aether.ChooseWireVersions(meshWireVersions(nil)[1:], meshWireVersions(nil)[1:])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := noCodec.Version(gossip.WireVersionCodec); ok {
		t.Fatal("premise wrong: the reduced set still chooses gossip.codec")
	}
	for name, mk := range map[string]func(*probeSession) aether.Session{
		"no gossip.codec chosen": func(p *probeSession) aether.Session { return reportingSession{p, noCodec} },
		"empty set":              func(p *probeSession) aether.Session { return reportingSession{p, aether.NegotiatedWireVersions{}} },
		"no set reported":        func(p *probeSession) aether.Session { return silentSession{p} },
	} {
		t.Run(name, func(t *testing.T) {
			m := acceptTestManager()
			defer stopProving(t, m)
			probe := wsSession()
			budgetBefore := m.budget.CurrentTotal()
			err := m.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
				NodeID: testNodeIDB, Proto: ProtoWebSocket, Session: mk(probe), IsInitiator: true,
			})
			if !errors.Is(err, gossip.ErrWireProfile) {
				t.Fatalf("err = %v, want gossip.ErrWireProfile", err)
			}
			m.mu.Lock()
			_, known := m.peers[testNodeIDB]
			m.mu.Unlock()
			if known {
				t.Fatal("the refused session's peer is in the connection table")
			}
			if got := m.budget.CurrentTotal(); got != budgetBefore {
				t.Fatalf("budget %d -> %d for a refused session", budgetBefore, got)
			}
			m.dispatchMu.RLock()
			_, registered := m.meshSessions[testNodeIDB]
			m.dispatchMu.RUnlock()
			if registered {
				t.Fatal("the refused session is in the dispatch table")
			}
			if probe.closeCalls.Load() != 1 {
				t.Fatalf("the refused session was closed %d times, want once", probe.closeCalls.Load())
			}
		})
	}
}

// startWireTestRuntime starts a runtime on a loopback VL1 port and returns it
// with that port.
func startWireTestRuntime(t *testing.T) (*Runtime, int) {
	t.Helper()
	t.Setenv(EnvNodeIdentitySeed, "")
	t.Setenv(EnvFlyMachineID, "")
	dir := t.TempDir()
	port := freeUDPPort(t)
	rt, err := Initialize(Config{
		ServiceName: "wire-versions-test",
		DataDir:     dir,
		NetworkKeys: []string{"wire-versions-test-network-key"},
		VL1:         VL1TransportConfig{Enabled: true, UDPPort: port},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt, port
}

// peerTransport is a Noise transport on the runtime's network key that offers
// the given application entries, as another node's build would.
func peerTransport(t *testing.T, app aether.WireVersions) (*noise.NoiseTransport, aether.NodeID, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := noise.NewNoiseTransport(noise.NoiseTransportConfig{
		PrivateKey: priv, LocalNode: id, ListenAddr: "127.0.0.1:0",
		NetworkKeys: []string{"wire-versions-test-network-key"}, WireVersions: app,
	})
	if err != nil {
		t.Fatalf("peer transport: %v", err)
	}
	ln, err := tr.Listen(context.Background())
	if err != nil {
		t.Fatalf("peer listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close(); _ = tr.Close() })
	return tr, id, ln.Addr().String()
}

// A runtime's VL1 Noise transport offers loom's entries in its handshake, so a
// node whose gossip.codec or streams.registry differs is refused in the
// handshake, whichever side dials, and a node of this build is admitted: the
// runtime's AcceptMeshConnection reads the gossip profile from the Noise
// session's chosen set and registers the session.
func TestNoiseMeshTransportRefusesAPeerWhoseLoomEntryDiffers(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a runtime")
	}
	rt, port := startWireTestRuntime(t)
	rtTarget := aether.Target{NodeID: rt.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", port)}
	dial := func(tr *noise.NoiseTransport, target aether.Target) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := tr.Dial(ctx, target)
		if c != nil {
			_ = c.Close()
		}
		return err
	}
	for _, name := range []string{gossip.WireVersionCodec, streams.WireVersionRegistry} {
		tr, id, addr := peerTransport(t, withLoomEntryAt(t, name, 2))
		if err := dial(tr, rtTarget); !errors.Is(err, aether.ErrWireVersionMismatch) {
			t.Errorf("%s differs, peer dials the runtime: err = %v, want ErrWireVersionMismatch", name, err)
		}
		if err := dial(rt.unpartitionedTransport, aether.Target{NodeID: id, Address: addr}); !errors.Is(err, aether.ErrWireVersionMismatch) {
			t.Errorf("%s differs, the runtime dials the peer: err = %v, want ErrWireVersionMismatch", name, err)
		}
	}
	tr, id, _ := peerTransport(t, meshWireVersions(nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := tr.Dial(ctx, rtTarget)
	if err != nil {
		t.Fatalf("a node of this build: dial: %v", err)
	}
	defer c.Close()
	waitMeshDB(t, 5*time.Second, "the runtime to register the admitted session", func() bool {
		return holdsOpenMeshSession(rt, string(id))
	})
}
