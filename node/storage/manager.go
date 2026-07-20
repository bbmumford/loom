/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"errors"
	"fmt"
	"io"
)

// Manager wires together store, chain and fetchers to provide higher-level helpers
// for nodes and leeches. It owns the lifecycle of the configured store backend.
type Manager struct {
	cfg      Config
	store    StoreBackend
	chain    ChainBackend
	fetchers []FetchBackend
}

// ManagerOption customises manager construction.
type ManagerOption func(*managerConfig)

type managerConfig struct {
	store    StoreBackend
	fetchers []FetchBackend
}

// WithManagerStore overrides the configured store backend.
func WithManagerStore(store StoreBackend) ManagerOption {
	return func(cfg *managerConfig) {
		cfg.store = store
	}
}

// WithManagerFetchers appends fetch backends shared by nodes and leeches.
func WithManagerFetchers(fetchers ...FetchBackend) ManagerOption {
	return func(cfg *managerConfig) {
		cfg.fetchers = append(cfg.fetchers, fetchers...)
	}
}

// NewManager builds a Manager using the provided configuration and chain backend.
func NewManager(cfg Config, chain ChainBackend, opts ...ManagerOption) (*Manager, error) {
	if chain == nil {
		return nil, errors.New("storage: chain backend is required")
	}

	optState := managerConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&optState)
		}
	}

	store := optState.store
	var err error
	if store == nil {
		store, err = cfg.Store.Build()
		if err != nil {
			return nil, fmt.Errorf("storage: build store: %w", err)
		}
	}

	manager := &Manager{
		cfg:      cfg,
		store:    store,
		chain:    chain,
		fetchers: append([]FetchBackend(nil), optState.fetchers...),
	}

	return manager, nil
}

// Store exposes the underlying store backend.
func (m *Manager) Store() StoreBackend { return m.store }

// Chain exposes the underlying chain backend.
func (m *Manager) Chain() ChainBackend { return m.chain }

// Fetchers returns the registered fetch backends.
func (m *Manager) Fetchers() []FetchBackend {
	return append([]FetchBackend(nil), m.fetchers...)
}

// Config returns the configuration used to construct the manager.
func (m *Manager) Config() Config { return m.cfg }

// Close releases resources owned by the manager.
func (m *Manager) Close() error {
	if closer, ok := m.store.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// NewNode constructs a Node pre-configured with the manager's store, chain and fetchers.
func (m *Manager) NewNode(opts ...NodeOption) (*Node, error) {
	opts = m.appendNodeDefaults(opts)
	return NewNode(m.store, m.chain, opts...)
}

// NewLeech constructs a Leech pre-configured with the manager's store, chain and fetchers.
func (m *Manager) NewLeech(opts ...LeechOption) (*Leech, error) {
	opts = m.appendLeechDefaults(opts)
	return NewLeech(m.store, m.chain, opts...)
}

func (m *Manager) appendNodeDefaults(opts []NodeOption) []NodeOption {
	with := make([]NodeOption, 0, len(opts)+1)
	if len(m.fetchers) > 0 {
		with = append(with, WithFetchers(m.fetchers...))
	}
	return append(with, opts...)
}

func (m *Manager) appendLeechDefaults(opts []LeechOption) []LeechOption {
	with := make([]LeechOption, 0, len(opts)+1)
	if len(m.fetchers) > 0 {
		with = append(with, WithLeechFetchers(m.fetchers...))
	}
	return append(with, opts...)
}
