/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

// Config captures runtime configuration for the storage manager.
type Config struct {
	Store StoreConfig `json:"store" yaml:"store"`
}

// StoreConfig describes the content-addressable storage provider.
type StoreConfig struct {
	Provider string           `json:"provider" yaml:"provider"`
	Disk     *DiskStoreConfig `json:"disk,omitempty" yaml:"disk,omitempty"`
}

// DiskStoreConfig configures the on-disk store provider.
type DiskStoreConfig struct {
	Root       string `json:"root" yaml:"root"`
	ShardDepth int    `json:"shardDepth,omitempty" yaml:"shardDepth,omitempty"`
	DirMode    string `json:"dirMode,omitempty" yaml:"dirMode,omitempty"`
	FileMode   string `json:"fileMode,omitempty" yaml:"fileMode,omitempty"`
}

// Validate ensures the configuration can produce working backends.
func (c Config) Validate() error {
	if _, err := c.Store.Build(); err != nil {
		return err
	}
	return nil
}

// Build instantiates a StoreBackend from the configuration.
func (c StoreConfig) Build() (StoreBackend, error) {
	provider := strings.ToLower(c.Provider)
	if provider == "" {
		if c.Disk != nil {
			provider = "disk"
		} else {
			return nil, fmt.Errorf("storage: store provider not specified")
		}
	}

	switch provider {
	case "disk":
		if c.Disk == nil {
			return nil, fmt.Errorf("storage: disk provider missing configuration")
		}
		return c.Disk.build()
	default:
		return nil, fmt.Errorf("storage: unsupported store provider %q", provider)
	}
}

func (cfg *DiskStoreConfig) build() (StoreBackend, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("storage: disk store root is required")
	}

	opts := make([]DiskStoreOption, 0, 3)
	if cfg.ShardDepth != 0 {
		opts = append(opts, WithShardDepth(cfg.ShardDepth))
	}

	if perm, err := parseFileMode(cfg.DirMode); err != nil {
		return nil, err
	} else if perm != 0 {
		opts = append(opts, WithDirectoryPerm(perm))
	}

	if perm, err := parseFileMode(cfg.FileMode); err != nil {
		return nil, err
	} else if perm != 0 {
		opts = append(opts, WithFilePerm(perm))
	}

	return NewDiskStore(cfg.Root, opts...)
}

func parseFileMode(mode string) (fs.FileMode, error) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return 0, nil
	}
	if !strings.HasPrefix(mode, "0") {
		mode = "0" + mode
	}
	value, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("storage: invalid file mode %q: %w", mode, err)
	}
	return fs.FileMode(value), nil
}
