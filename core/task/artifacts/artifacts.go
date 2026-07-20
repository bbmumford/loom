/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package artifacts

import (
	"encoding/json"
	"fmt"

	storage "github.com/bbmumford/loom/node/storage"
)

// ArtifactPurpose classifies how an artifact should be interpreted by an executor.
type ArtifactPurpose string

const (
	ArtifactPurposeInput      ArtifactPurpose = "input"
	ArtifactPurposeOutput     ArtifactPurpose = "output"
	ArtifactPurposeCheckpoint ArtifactPurpose = "checkpoint"
	ArtifactPurposeLog        ArtifactPurpose = "log"
)

// ManifestRef references a manifest stored via the storage package while
// exposing a hex string representation for JSON/YAML payloads.
type ManifestRef struct {
	ID storage.ManifestID
}

// NewManifestRef constructs a manifest reference from the provided ID.
func NewManifestRef(id storage.ManifestID) ManifestRef {
	return ManifestRef{ID: id}
}

// ManifestID returns the underlying manifest identifier.
func (m ManifestRef) ManifestID() storage.ManifestID {
	return m.ID
}

// Hex returns the hexadecimal representation of the manifest identifier.
func (m ManifestRef) Hex() string {
	return m.ID.Hex()
}

// IsZero reports whether the manifest identifier is the zero value.
func (m ManifestRef) IsZero() bool {
	return m.ID == (storage.ManifestID{})
}

// MarshalJSON encodes the manifest reference as a hex string.
func (m ManifestRef) MarshalJSON() ([]byte, error) {
	if m.IsZero() {
		return json.Marshal("")
	}
	return json.Marshal(m.ID.Hex())
}

// UnmarshalJSON decodes a manifest reference from a hex string.
func (m *ManifestRef) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		m.ID = storage.ManifestID{}
		return nil
	}
	h, err := storage.HashFromHex(s)
	if err != nil {
		return fmt.Errorf("task: decode manifest ref: %w", err)
	}
	m.ID = storage.ManifestID(h)
	return nil
}

// Artifact represents an external payload associated with a task (inputs,
// outputs, checkpoints, logs). Each artifact references a storage manifest.
type Artifact struct {
	Name        string            `json:"name"`
	Purpose     ArtifactPurpose   `json:"purpose"`
	Reference   ManifestRef       `json:"reference"`
	SizeBytes   int64             `json:"sizeBytes,omitempty"`
	ContentType string            `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Copy returns a deep copy of the artifact, including metadata.
func (a Artifact) Copy() Artifact {
	dup := a
	if a.Metadata != nil {
		dup.Metadata = make(map[string]string, len(a.Metadata))
		for k, v := range a.Metadata {
			dup.Metadata[k] = v
		}
	}
	return dup
}

func cloneArtifacts(src []Artifact) []Artifact {
	if len(src) == 0 {
		return nil
	}
	dup := make([]Artifact, len(src))
	for i, art := range src {
		dup[i] = art.Copy()
	}
	return dup
}
