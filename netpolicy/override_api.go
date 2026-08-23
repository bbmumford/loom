/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"encoding/json"
	"net/http"
	"strings"
)

// OverrideAPI is the mesh-debug surface for per-peer network-profile overrides: an operator can
// inspect the profile in force for one peer, pin a tighter (or looser) one at runtime — e.g. shorter
// retries for a known-flaky peer — or clear it. It wraps a live Policy and is self-contained;
// mounting it on the node's mesh-debug router (under some prefix) is the only integration step. The
// handler manipulates only per-peer overrides, never the global synthesized profile, so a debug
// override can never widen the whole node's posture by accident.
//
// Routes, relative to the mount Prefix:
//
//	GET    <prefix>/<peer>  → the effective NetworkProfile for the peer (its override, else the global)
//	PUT    <prefix>/<peer>  → set an override from a NetworkProfile JSON body
//	DELETE <prefix>/<peer>  → clear the override (revert the peer to the global profile)
type OverrideAPI struct {
	Policy *Policy
	Prefix string
}

func (a *OverrideAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.Policy == nil {
		http.Error(w, "no policy", http.StatusServiceUnavailable)
		return
	}
	peer := strings.Trim(strings.TrimPrefix(r.URL.Path, a.Prefix), "/")
	if peer == "" {
		http.Error(w, "peer id required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(a.Policy.PeerOverride(peer))
	case http.MethodPut:
		var prof NetworkProfile
		if err := json.NewDecoder(r.Body).Decode(&prof); err != nil {
			http.Error(w, "malformed profile", http.StatusBadRequest)
			return
		}
		a.Policy.SetPeerOverride(peer, prof)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		a.Policy.SetPeerOverride(peer, NetworkProfile{})
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
