/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ledger

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	ladstorage "github.com/bbmumford/ledger/storage"
	_ "turso.tech/database/tursogo"
)

// openLADDB opens an empty embedded database of the kind the mesh ledger's
// manager hands NewMeshLedger.
func openLADDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("turso", filepath.Join(t.TempDir(), "lad.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func createLADRecords(t *testing.T, db *sql.DB, partitionColumn string) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE lad_records (
		topic TEXT NOT NULL,
		seq INTEGER NOT NULL,
		` + partitionColumn + ` TEXT NOT NULL,
		node_id TEXT NOT NULL,
		body TEXT NOT NULL,
		signature BLOB,
		timestamp TEXT NOT NULL,
		PRIMARY KEY (topic, seq)
	)`); err != nil {
		t.Fatal(err)
	}
}

func TestRequirePartitionColumnAdmitsAMissingTable(t *testing.T) {
	if err := ladstorage.RequirePartitionColumn(context.Background(), openLADDB(t)); err != nil {
		t.Fatalf("empty database refused: %v", err)
	}
}

func TestRequirePartitionColumnAdmitsAPartitionTable(t *testing.T) {
	db := openLADDB(t)
	createLADRecords(t, db, "partition_id")
	if err := ladstorage.RequirePartitionColumn(context.Background(), db); err != nil {
		t.Fatalf("partition_id table refused: %v", err)
	}
}

// A table that keeps the partition in tenant_id is refused with an error that
// names the column and tells the operator to delete the database, instead of
// failing later on the partition_id index.
func TestRequirePartitionColumnRefusesATenantColumnTable(t *testing.T) {
	db := openLADDB(t)
	createLADRecords(t, db, "tenant_id")
	err := ladstorage.RequirePartitionColumn(context.Background(), db)
	if err == nil {
		t.Fatal("tenant_id table admitted; the partition_id index would fail on it")
	}
	if !strings.Contains(err.Error(), "tenant_id") || !strings.Contains(err.Error(), "delete the ledger database") {
		t.Fatalf("error = %q, want it to name tenant_id and the fix", err)
	}
}
