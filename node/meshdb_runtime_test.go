/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/ledger/storage"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/swarm"
	"github.com/bbmumford/tursoraft/localdb"
	meshdbcfg "github.com/bbmumford/tursoraft/manager"
)

// unclaimedStream records whether the RPC server read it and how often it
// was reset.
type unclaimedStream struct {
	*fakeStream
	mu       sync.Mutex
	resets   int
	received bool
}

func (s *unclaimedStream) Reset(aether.ResetReason) error {
	s.mu.Lock()
	s.resets++
	s.mu.Unlock()
	return nil
}

func (s *unclaimedStream) Receive(context.Context) ([]byte, error) {
	s.mu.Lock()
	s.received = true
	s.mu.Unlock()
	return nil, io.EOF
}

// An application-band stream nobody claimed is reset, never parsed as RPC; a
// dynamic RPC stream below the band is served.
func TestUnclaimedApplicationStreamIsRefusedNotServedAsRPC(t *testing.T) {
	srv := NewRPCServer(handlers.NewHandlerRegistry())
	for _, tc := range []struct {
		id        uint64
		wantRPC   bool
		wantReset int
	}{
		{id: 101, wantRPC: false, wantReset: 1},
		{id: 42, wantRPC: true, wantReset: 0},
	} {
		st := &unclaimedStream{fakeStream: newFakeStream(tc.id)}
		srv.serveUnclaimedStream(context.Background(), st, "vl1_peer")
		if st.received != tc.wantRPC || st.resets != tc.wantReset {
			t.Errorf("stream %d: served as RPC=%v resets=%d, want %v and %d", tc.id, st.received, st.resets, tc.wantRPC, tc.wantReset)
		}
	}
}

func testGroupKey(t *testing.T) string {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(k)
}

func validDatabasesTestConfig(t *testing.T) Config {
	dir := t.TempDir()
	return Config{
		ServiceName: meshDBTestService,
		DataDir:     dir,
		NetworkKeys: []string{"k"},
		VL1:         VL1TransportConfig{Enabled: true},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
		Databases: &DatabasesConfig{
			Enabled:  true,
			LocalDir: filepath.Join(dir, "db"),
			Groups: []meshdbcfg.GroupConfig{{
				GroupName:     "control",
				DBNames:       []string{"ctl"},
				EncryptionKey: testGroupKey(t),
				Raft:          &meshdbcfg.RaftSpec{VoterServices: []string{meshDBTestService}, VoterCount: 3, BootstrapExpect: 3},
			}},
		},
	}
}

// Validate refuses what tursoraft and the LAD file would refuse later, before
// anything is opened.
func TestValidateRefusesRelativePathsMissingKeysAndMeshLAD(t *testing.T) {
	if cfg := validDatabasesTestConfig(t); cfg.Validate() != nil {
		t.Fatalf("control config refused: %v", cfg.Validate())
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"relative databases dir", func(c *Config) { c.Databases.LocalDir = "./data/db" }, "absolute"},
		{"relative volume dir", func(c *Config) { c.Databases.VolumeDir = "data" }, "absolute"},
		{"relative LAD dir", func(c *Config) { c.LAD.LocalDir = "./data/lad" }, "absolute"},
		{"mesh LAD storage", func(c *Config) { c.LAD.Storage = "mesh" }, "LAD storage"},
		{"group without a key", func(c *Config) { c.Databases.Groups[0].EncryptionKey = "" }, "no encryption key"},
		{"raft without VL1", func(c *Config) { c.VL1.Enabled = false }, "need VL1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validDatabasesTestConfig(t)
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want an error containing %q", err, tc.want)
			}
		})
	}
	cfg := validDatabasesTestConfig(t)
	cfg.Databases.Groups[0].EncryptionKey = ""
	cfg.Databases.KeyResolver = func(string) (string, string, error) { return "", "", nil }
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a group keyed by the resolver was refused: %v", err)
	}
}

// KeyResolver fills every group's keys; its error or an empty key fails.
func TestResolvedGroupsTakeTheResolverKeys(t *testing.T) {
	cur, prior := testGroupKey(t), testGroupKey(t)
	dc := &DatabasesConfig{Groups: []meshdbcfg.GroupConfig{{GroupName: "control", EncryptionKey: "stale"}}}
	dc.KeyResolver = func(group string) (string, string, error) { return cur, prior, nil }
	groups, err := dc.resolvedGroups()
	if err != nil || groups[0].EncryptionKey != cur || groups[0].PriorEncryptionKey != prior {
		t.Fatalf("resolvedGroups = %+v, %v", groups, err)
	}
	if dc.Groups[0].EncryptionKey != "stale" {
		t.Fatal("resolvedGroups changed the configured groups")
	}
	dc.KeyResolver = func(string) (string, string, error) { return "", "", errors.New("vault down") }
	if _, err := dc.resolvedGroups(); err == nil {
		t.Fatal("a resolver error did not fail")
	}
	dc.KeyResolver = func(string) (string, string, error) { return "", "", nil }
	if _, err := dc.resolvedGroups(); err == nil {
		t.Fatal("an empty resolved key did not fail")
	}
}

// Each raft block seats its voter services and its job hosts under the key
// tursoraft's MeshKey gives its cluster; a group without one has no key.
func TestMeshDBSeatsFollowTheRaftBlocks(t *testing.T) {
	seats := meshDBSeats([]meshdbcfg.GroupConfig{
		{GroupName: "control", DBNames: []string{"ctl"}, Raft: &meshdbcfg.RaftSpec{
			VoterServices: []string{"node.a"}, Jobs: map[string][]string{"projector": {"api.a"}},
		}},
		{GroupName: "sessions", DBNames: []string{"s"}},
	})
	key := meshdbcfg.MeshKey("control", "ctl")
	want := map[string]map[string]bool{key: {"node.a": true, "api.a": true}}
	if len(seats) != 1 || len(seats[key]) != 2 || !seats[key]["node.a"] || !seats[key]["api.a"] {
		t.Fatalf("meshDBSeats = %v, want %v", seats, want)
	}
	if _, ok := seats[meshdbcfg.MeshKey("sessions", "s")]; ok {
		t.Fatalf("meshDBSeats seats a group with no raft block: %v", seats)
	}
}

// A slot under this NodeID that this process never published is a previous
// process's leftover and is withdrawn; a slot it did publish is kept.
func TestMeshDBDirectoryWithdrawsALeftoverSelfSlot(t *testing.T) {
	d := newMeshDBDirectory("vl1_self", nil)
	withdrawn := make(chan string, 2)
	d.withdrawFn = func(key string) error { withdrawn <- key; return nil }
	d.publishFn = func(string, []byte) error { return nil }
	if err := d.publish("tursoraft/g/current", map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	d.onMemberRecord(swarm.Record{NodeID: "vl1_self", Key: "tursoraft/g/current", Body: []byte(`{"role":"voter"}`)})
	d.onMemberRecord(swarm.Record{NodeID: "vl1_self", Key: "tursoraft/g/leftover", Body: []byte(`{"role":"voter"}`)})
	select {
	case key := <-withdrawn:
		if key != "tursoraft/g/leftover" {
			t.Fatalf("withdrew %s, want only the leftover slot", key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the leftover slot was not withdrawn")
	}
	d.close()
	if len(withdrawn) != 0 {
		t.Fatalf("withdrew a slot this process published: %s", <-withdrawn)
	}
}

// A leftover slot's withdrawal, which runs on its own goroutine, never
// reaches the mesh after this process publishes the same key: peers would
// hold the tombstone while this process believes the slot is published.
func TestMeshDBDirectoryLeftoverWithdrawalNeverFollowsAPublish(t *testing.T) {
	const key = "tursoraft/g/db"
	for i := 0; i < 200; i++ {
		d := newMeshDBDirectory("vl1_self", nil)
		var mu sync.Mutex
		var ops []string
		record := func(op string) error {
			mu.Lock()
			ops = append(ops, op)
			mu.Unlock()
			return nil
		}
		d.publishFn = func(string, []byte) error { return record("publish") }
		d.withdrawFn = func(string) error { return record("withdraw") }
		d.onMemberRecord(swarm.Record{NodeID: "vl1_self", Key: key, Body: []byte(`{"role":"voter"}`)})
		if err := d.publish(key, map[string]string{"role": "voter"}); err != nil {
			t.Fatal(err)
		}
		d.close()
		if ops[len(ops)-1] != "publish" {
			t.Fatalf("iteration %d: the slot's operations %v end in a withdrawal", i, ops)
		}
	}
}

// The service rides the PeerRecord from InitSwarm on, before the endpoint
// publishes its handlers.
func TestInitSwarmAdvertisesTheServiceFromBoot(t *testing.T) {
	rt := newPeerPublicationTestRuntime(t)
	if _, err := rt.InitSwarm(rpc.NewRegistry()); err != nil {
		t.Fatalf("InitSwarm: %v", err)
	}
	rt.swarm.Publisher.mu.Lock()
	got := rt.swarm.Publisher.serviceName
	rt.swarm.Publisher.mu.Unlock()
	if got != rt.cfg.ServiceName {
		t.Fatalf("publisher service = %q after InitSwarm, want %q", got, rt.cfg.ServiceName)
	}
}

func testLADKey(t *testing.T) []byte {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ladFileKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func fileHeader(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	head := make([]byte, 16)
	if _, err := io.ReadFull(f, head); err != nil {
		t.Fatal(err)
	}
	return head
}

// The LAD file carries the ledger's schema under the node's key: it has no
// SQLite header and no plaintext row, and reopens under the same key.
func TestLADFileIsEncryptedUnderTheNodeKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := testLADKey(t)
	db, err := openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("openLADFile: %v", err)
	}
	if _, err := storage.NewLocalDBLedger(db, storage.LedgerConfig{RetentionDays: 30}); err != nil {
		t.Fatalf("the LAD ledger schema on the encrypted file: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE marker(v TEXT); INSERT INTO marker VALUES ('lad-plaintext-marker')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	path := filepath.Join(dir, ladFileName)
	if bytes.HasPrefix(fileHeader(t, path), []byte("SQLite format 3")) {
		t.Fatal("the LAD file has a plaintext SQLite header")
	}
	for _, p := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(p)
		if err == nil && bytes.Contains(raw, []byte("lad-plaintext-marker")) {
			t.Fatalf("%s holds a row in plaintext", p)
		}
	}
	db, err = openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("reopen under the same key: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM marker`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("reopened file lost its rows: n=%d err=%v", n, err)
	}
}

// A plaintext LAD file, or one written under another node's key, is replaced
// by an empty encrypted file.
func TestLADFileThisNodeCannotReadIsReplaced(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, path string)
	}{
		{"plaintext", func(t *testing.T, path string) {
			// "turso" is the tursogo driver localdb registers; with no key in
			// the name it writes a plaintext SQLite file.
			db, err := sql.Open("turso", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`CREATE TABLE old(v TEXT)`); err != nil {
				t.Fatal(err)
			}
			db.Close()
			if !bytes.HasPrefix(fileHeader(t, path), []byte("SQLite format 3")) {
				t.Fatal("the plaintext fixture has no plaintext SQLite header")
			}
		}},
		{"another identity's key", func(t *testing.T, path string) {
			db, err := localdb.Open(ctx, path, testLADKey(t))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`CREATE TABLE old(v TEXT)`); err != nil {
				t.Fatal(err)
			}
			db.Close()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, ladFileName)
			tc.write(t, path)
			db, err := openLADFile(ctx, dir, testLADKey(t))
			if err != nil {
				t.Fatalf("openLADFile over an unreadable file: %v", err)
			}
			defer db.Close()
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'old'`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("the unreadable file was kept: n=%d err=%v", n, err)
			}
			if _, err := db.Exec(`CREATE TABLE fresh(v TEXT)`); err != nil {
				t.Fatal(err)
			}
			if bytes.HasPrefix(fileHeader(t, path), []byte("SQLite format 3")) {
				t.Fatal("the replacement file is plaintext")
			}
		})
	}
}

// A LAD file that fails to open for a reason other than what it holds is
// returned as that error and kept: here this process may not read it, which
// says nothing about its contents, and a later start with the right
// permissions reads it again.
func TestLADFileOpenErrorKeepsTheFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a file whatever its mode")
	}
	ctx := context.Background()
	dir := t.TempDir()
	key := testLADKey(t)
	db, err := openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("openLADFile: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE kept(v TEXT)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	path := filepath.Join(dir, ladFileName)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(path, 0o600) })

	if db, err := openLADFile(ctx, dir, key); err == nil {
		db.Close()
		t.Fatal("openLADFile over a file it may not read succeeded: the file was replaced")
	} else if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("openLADFile = %v, want the permission error", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err = openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("reopen once readable: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'kept'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the file was not kept: n=%d err=%v", n, err)
	}
}
