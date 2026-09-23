/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"path/filepath"
	"time"

	meshdbcfg "github.com/bbmumford/tursoraft/manager"
)

// DatabasesConfig configures the tursoraft database manager: encrypted
// embedded replicas of shared databases, encrypted local files, and one Raft
// cluster per group with a raft block, run over the endpoint mesh by
// MeshDBTransport.
type DatabasesConfig struct {
	Enabled bool

	// LocalDir is the absolute directory every database file and every
	// voter's Raft state lives under.
	LocalDir string
	// VolumeDir is the absolute root of the persistent volume. tursoraft
	// requires a process whose service votes in any cluster to keep
	// LocalDir under it, because a voter that loses its Raft state on
	// restart rejoins as a stranger. Empty means Config.DataDir when that is
	// absolute.
	VolumeDir string
	// SyncInterval is the pull period of a group that sets none.
	SyncInterval time.Duration

	// Groups are the database groups this process holds. A group's Raft
	// field declares its cluster: voterServices, voterCount,
	// bootstrapExpect and the jobs with their host services.
	Groups []meshdbcfg.GroupConfig

	// KeyResolver, when set, supplies each group's key and its prior key
	// during a rotation (base64 of 32 bytes each; prior may be empty) and
	// replaces the group's EncryptionKey and PriorEncryptionKey. An error,
	// or an empty current key, fails Initialize. When nil, each group's own
	// EncryptionKey is used and a group without one is refused.
	KeyResolver func(group string) (current, prior string, err error)

	// RaftBindAddr is ignored: Raft runs over the endpoint mesh.
	//
	// Deprecated: ignored; set by library/endpoint and node.orbtr.io.
	RaftBindAddr string
	// SnapshotDir is ignored: Raft snapshots sit under LocalDir.
	//
	// Deprecated: ignored; set by library/endpoint and node.orbtr.io.
	SnapshotDir string
	// TursoAPIToken and TursoOrgName are ignored: every shared database
	// opens from its own url and token.
	//
	// Deprecated: ignored; set by library/endpoint and node.orbtr.io.
	TursoAPIToken string
	TursoOrgName  string
}

// hasRaft reports whether any group declares a raft block, which is what
// needs MeshDBTransport.
func (d *DatabasesConfig) hasRaft() bool {
	if d == nil || !d.Enabled {
		return false
	}
	for _, g := range d.Groups {
		if g.Raft != nil {
			return true
		}
	}
	return false
}

// validate checks what Initialize needs before it opens anything: absolute
// directories, a key source for every group, and VL1 for any cluster.
func (d *DatabasesConfig) validate(c *Config) error {
	if d == nil || !d.Enabled {
		return nil
	}
	if d.LocalDir == "" {
		return fmt.Errorf("databases local directory is required when databases are enabled")
	}
	if !filepath.IsAbs(d.LocalDir) {
		return fmt.Errorf("databases local directory %q must be an absolute path", d.LocalDir)
	}
	if d.VolumeDir != "" && !filepath.IsAbs(d.VolumeDir) {
		return fmt.Errorf("databases volume directory %q must be an absolute path", d.VolumeDir)
	}
	if d.KeyResolver == nil {
		for _, g := range d.Groups {
			if g.EncryptionKey == "" {
				return fmt.Errorf("databases group %q has no encryption key and no key resolver is configured", g.GroupName)
			}
		}
	}
	if d.hasRaft() && !c.VL1.Enabled {
		return fmt.Errorf("databases groups with a raft block need VL1: their clusters run over the endpoint mesh")
	}
	return nil
}

// volumeDir is the volume root passed to tursoraft.
func (d *DatabasesConfig) volumeDir(dataDir string) string {
	if d.VolumeDir != "" {
		return d.VolumeDir
	}
	if filepath.IsAbs(dataDir) {
		return dataDir
	}
	return ""
}

// resolvedGroups returns the groups with their keys filled from KeyResolver,
// or the groups as configured when there is none.
func (d *DatabasesConfig) resolvedGroups() ([]meshdbcfg.GroupConfig, error) {
	groups := make([]meshdbcfg.GroupConfig, len(d.Groups))
	copy(groups, d.Groups)
	if d.KeyResolver == nil {
		return groups, nil
	}
	for i := range groups {
		current, prior, err := d.KeyResolver(groups[i].GroupName)
		if err != nil {
			return nil, fmt.Errorf("databases group %q: resolve key: %w", groups[i].GroupName, err)
		}
		if current == "" {
			return nil, fmt.Errorf("databases group %q: key resolver returned no key", groups[i].GroupName)
		}
		groups[i].EncryptionKey = current
		groups[i].PriorEncryptionKey = prior
	}
	return groups, nil
}

// meshDBSeats maps each cluster key the groups declare to the services with
// a seat in it: its voter services and every job's host services. A group
// with a raft block holds exactly one database (tursoraft refuses any other
// shape), named first in DBNames, else in Databases.
//
// The key is tursoraft's MeshKey, the key the cluster publishes its members
// and dials its streams under. MeshDBTransport admits an inbound stream only
// when its key is in this map, so a key formed any other way admits nobody.
func meshDBSeats(groups []meshdbcfg.GroupConfig) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, g := range groups {
		if g.Raft == nil {
			continue
		}
		db := ""
		if len(g.DBNames) > 0 {
			db = g.DBNames[0]
		} else if len(g.Databases) > 0 {
			db = g.Databases[0].Name
		}
		if db == "" {
			continue
		}
		seats := map[string]bool{}
		for _, s := range g.Raft.VoterServices {
			seats[s] = true
		}
		for _, hosts := range g.Raft.Jobs {
			for _, s := range hosts {
				seats[s] = true
			}
		}
		out[meshdbcfg.MeshKey(g.GroupName, db)] = seats
	}
	return out
}
