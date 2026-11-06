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
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

// The seed side of the join proof, driven by a joiner written here from the wire
// contract alone (headers, transcript, proof layout), so the seed's checks are
// judged by bytes a peer sends rather than by the package's own joiner.

const seedJoinTestKey = "seed-join-test-network-key"

// seedJoinTestRuntime starts a runtime on its own loopback UDP port holding
// networkKey for the control-style partition "orbtr" on the shared transport,
// as an endpoint's spine configures one, with verify as its anchor verifier.
func seedJoinTestRuntime(t *testing.T, service, networkKey string, verify ports.VerifyBootstrapFunc) *Runtime {
	t.Helper()
	t.Setenv(EnvNodeIdentitySeed, "")
	t.Setenv(EnvFlyMachineID, "")
	dir := t.TempDir()
	rt, err := Initialize(Config{
		ServiceName:     service,
		DataDir:         dir,
		NetworkKeys:     []string{networkKey},
		Partitions:      []PartitionTransportConfig{{PartitionID: "orbtr", NetworkKeys: []string{networkKey}}},
		VL1:             VL1TransportConfig{Enabled: true, UDPPort: freeUDPPort(t)},
		LAD:             LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
		VerifyBootstrap: verify,
	})
	if err != nil {
		t.Fatalf("Initialize %s: %v", service, err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt
}

// seedJoinTestServer serves rt's /mesh/vl1 over TLS and returns its host and
// the roots that trust its certificate.
func seedJoinTestServer(t *testing.T, rt *Runtime) (string, *x509.CertPool) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/mesh/vl1", rt.BootstrapHandler())
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	return srv.Listener.Addr().String(), roots
}

// rawJoiner is what a joiner sends: the node ID it states, the identity key it
// proves with, the partition it names, the network key its HMAC is made under,
// and whether it sends the proof headers at all.
type rawJoiner struct {
	statedID  string
	identity  ed25519.PrivateKey
	partition string
	macKey    []byte
	noHeaders bool
}

func newRawJoiner(t *testing.T, macKey string) rawJoiner {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return rawJoiner{statedID: string(id), identity: priv, partition: "orbtr", macKey: []byte(macKey)}
}

// rawTranscript is the transcript encoding of the wire contract: each field as a
// 4-byte big-endian length and its bytes.
func rawTranscript(fields ...[]byte) []byte {
	var out []byte
	for _, f := range fields {
		out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
		out = append(out, f...)
	}
	return out
}

// join sends j's upgrade to host and, on a 101, j's proof, and reports the
// status and whether the seed answered the proof with the accepted byte.
func (j rawJoiner) join(t *testing.T, host string, roots *x509.CertPool) (int, bool) {
	t.Helper()
	conn, err := tls.Dial("tcp", host, &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"})
	if err != nil {
		t.Fatalf("dial seed: %v", err)
	}
	defer conn.Close()
	nonce := make([]byte, 32)
	_, _ = rand.Read(nonce)
	req, _ := http.NewRequest(http.MethodPost, "https://"+host+"/mesh/vl1", nil)
	req.Header.Set("Upgrade", "vl1")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("X-VL1-Node-ID", j.statedID)
	if !j.noHeaders {
		req.Header.Set("X-VL1-Join-Partition", j.partition)
		req.Header.Set("X-VL1-Join-Nonce", base64.RawURLEncoding.EncodeToString(nonce))
	}
	if err := req.Write(conn); err != nil {
		t.Fatalf("write upgrade: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("read upgrade answer: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return resp.StatusCode, false
	}
	seedNonce, _ := base64.RawURLEncoding.DecodeString(resp.Header.Get("X-VL1-Join-Nonce"))
	transcript := rawTranscript([]byte("loom.vl1.seed-join.v1"), []byte("joiner"), []byte(j.partition),
		[]byte(j.statedID), []byte(aether.NormalizeNodeID(resp.Header.Get("X-VL1-Node-ID"))), nonce, seedNonce)
	m := hmac.New(sha256.New, j.macKey)
	m.Write(transcript)
	proof := append(append(append([]byte(nil), j.identity.Public().(ed25519.PublicKey)...),
		ed25519.Sign(j.identity, transcript)...), m.Sum(nil)...)
	if _, err := conn.Write(proof); err != nil {
		return resp.StatusCode, false
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	b, err := br.ReadByte()
	return resp.StatusCode, err == nil && b == 1
}

// joinerState reports whether the seed recorded anything about nodeID: a held
// bootstrap connection, a mesh session, or gossip liveness.
func joinerState(seed *Runtime, nodeID string) []string {
	var held []string
	seed.bootstrapConnsMu.Lock()
	if len(seed.bootstrapConns) > 0 {
		held = append(held, "a bootstrap connection")
	}
	seed.bootstrapConnsMu.Unlock()
	if _, ok := seed.ConnManager().MeshSessionForNode(nodeID); ok {
		held = append(held, "a mesh session")
	}
	if seed.cache != nil && !seed.cache.LastGossipSeen(nodeID).IsZero() {
		held = append(held, "gossip liveness")
	}
	return held
}

// A joiner whose proof is made under a network key the seed does not hold is
// refused, and the seed records nothing about it, whatever the anchor verifier
// answers: with no verifier, with a verifier that cannot be reached (anchor.Client
// with no anchor answering), and with one that admits the join.
func TestASeedRefusesAJoinerOnAnotherNetworkKeyWhateverTheAnchorAnswers(t *testing.T) {
	unreachable := func(context.Context, ports.BootstrapInfo) (bool, string, error) {
		return false, "", errors.New("no anchor answered")
	}
	admits := func(context.Context, ports.BootstrapInfo) (bool, string, error) { return true, "", nil }
	for name, verify := range map[string]ports.VerifyBootstrapFunc{
		"no verifier": nil, "anchor unreachable": unreachable, "anchor admits": admits,
	} {
		t.Run(name, func(t *testing.T) {
			seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, verify)
			host, roots := seedJoinTestServer(t, seed)
			j := newRawJoiner(t, seedJoinTestKey+"-another")
			_, answered := j.join(t, host, roots)
			if answered {
				t.Error("the seed accepted a join proof made under another network key")
			}
			time.Sleep(500 * time.Millisecond)
			if held := joinerState(seed, j.statedID); len(held) > 0 {
				t.Errorf("after refusing the join the seed holds %v for the joiner", held)
			}
		})
	}
}

// The seed refuses, before the upgrade, a join that sends no proof headers and
// one naming a partition the seed holds no key for; and after the upgrade a
// proof whose key does not derive the node ID the joiner stated, so a key holder
// cannot join under another node's ID.
func TestASeedRefusesAJoinWithNoProofAnUnheldPartitionOrAnotherNodesID(t *testing.T) {
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, nil)
	host, roots := seedJoinTestServer(t, seed)

	noHeaders := newRawJoiner(t, seedJoinTestKey)
	noHeaders.noHeaders = true
	if status, _ := noHeaders.join(t, host, roots); status != http.StatusForbidden {
		t.Errorf("a join with no proof headers: status %d, want 403", status)
	}

	unheld := newRawJoiner(t, seedJoinTestKey)
	unheld.partition = "p-the-seed-does-not-hold"
	if status, _ := unheld.join(t, host, roots); status != http.StatusForbidden {
		t.Errorf("a join naming a partition the seed holds no key for: status %d, want 403", status)
	}

	other := newRawJoiner(t, seedJoinTestKey)
	impostor := newRawJoiner(t, seedJoinTestKey)
	impostor.statedID = other.statedID
	if _, answered := impostor.join(t, host, roots); answered {
		t.Error("the seed accepted a proof whose key does not derive the stated node ID")
	}

	time.Sleep(500 * time.Millisecond)
	for _, j := range []rawJoiner{noHeaders, unheld, other} {
		if held := joinerState(seed, j.statedID); len(held) > 0 {
			t.Errorf("the seed holds %v for a refused joiner %s", held, truncID(j.statedID))
		}
	}
}

// A joiner holding the seed's key for the partition is answered with the
// accepted byte.
func TestASeedAcceptsAJoinerProvingItsKeyForThePartition(t *testing.T) {
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, nil)
	host, roots := seedJoinTestServer(t, seed)
	j := newRawJoiner(t, seedJoinTestKey)
	if status, answered := j.join(t, host, roots); status != http.StatusSwitchingProtocols || !answered {
		t.Fatalf("a joiner on the seed's key: status %d, accepted %v; want 101 and accepted", status, answered)
	}
}
