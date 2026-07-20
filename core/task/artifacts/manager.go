/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package artifacts

import (
	"context"
	"io"

	"github.com/bbmumford/loom/node/storage"
)

// Manager handles artifact storage and retrieval.
type Manager struct {
	node  *storage.Node
	leech *storage.Leech
}

// NewManager creates a new artifact manager.
func NewManager(node *storage.Node, leech *storage.Leech) *Manager {
	return &Manager{
		node:  node,
		leech: leech,
	}
}

// UploadArtifact stores an artifact from a reader and returns its manifest ID.
func (m *Manager) UploadArtifact(ctx context.Context, r io.Reader, purpose ArtifactPurpose) (ManifestRef, error) {
	// We can add metadata based on purpose
	opts := storage.PublishOptions{
		Meta: map[string]string{
			"purpose": string(purpose),
		},
	}

	res, err := m.node.Publish(ctx, r, opts)
	if err != nil {
		return ManifestRef{}, err
	}

	return NewManifestRef(res.ManifestID), nil
}

// DownloadArtifact retrieves an artifact by manifest ID and writes it to w.
func (m *Manager) DownloadArtifact(ctx context.Context, ref ManifestRef, w io.Writer) error {
	_, err := m.leech.FetchByID(ctx, ref.ID, w)
	return err
}
