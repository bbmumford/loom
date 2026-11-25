/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bbmumford/filekind"
	"github.com/bbmumford/policypool"
	"github.com/bbmumford/tursoraft/localdb"
)

var ladHeader = filekind.Header{ApplicationID: filekind.Lad.ApplicationID(), UserVersion: 1}

// A new LAD file carries the lad kind, generation 1, in its header, and opens
// again under it with its rows.
func TestLADFileCarriesTheLADKind(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	key := testLADKey(t)
	db, err := openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("openLADFile: %v", err)
	}
	if h, err := filekind.Read(ctx, db); err != nil || h != ladHeader {
		t.Fatalf("a new LAD file's header = %+v, %v, want lad generation 1", h, err)
	}
	if _, err := db.Exec(`CREATE TABLE kept(v TEXT); INSERT INTO kept VALUES ('x')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = openLADFile(ctx, dir, key)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM kept`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("the reopened LAD file lost its rows: n=%d err=%v", n, err)
	}
}

// A LAD file this node can read but whose header is not the lad kind, one
// written before the header was stamped or one of another kind, is rebuilt
// as an empty LAD file carrying the lad kind.
func TestLADFileOfAnotherIdentityIsRebuilt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(ctx context.Context, path string, key []byte) error
	}{
		{"written before the stamp", func(ctx context.Context, path string, key []byte) error {
			db, err := localdb.Open(ctx, path, key)
			if err != nil {
				return err
			}
			defer db.Close()
			_, err = db.Exec(`CREATE TABLE old(v TEXT)`)
			return err
		}},
		{"another kind", func(ctx context.Context, path string, key []byte) error {
			db, err := localdb.OpenFile(ctx, path, key, policypool.Identity{Kind: filekind.TursoraftLocal, Generation: 1})
			if err != nil {
				return err
			}
			defer db.Close()
			_, err = db.Exec(`CREATE TABLE old(v TEXT)`)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			key := testLADKey(t)
			if err := tc.open(ctx, filepath.Join(dir, ladFileName), key); err != nil {
				t.Fatal(err)
			}
			db, err := openLADFile(ctx, dir, key)
			if err != nil {
				t.Fatalf("openLADFile over a file of another identity: %v", err)
			}
			defer db.Close()
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'old'`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("the file of another identity was kept: n=%d err=%v", n, err)
			}
			if h, err := filekind.Read(ctx, db); err != nil || h != ladHeader {
				t.Fatalf("the rebuilt LAD file's header = %+v, %v, want lad generation 1", h, err)
			}
		})
	}
}
