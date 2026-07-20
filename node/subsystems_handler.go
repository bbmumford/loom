/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bbmumford/loom/pkg/obshealth"
)

// SubsystemsResponse is the JSON body returned by SubsystemsHandler.
//
// Shape parallels the ORBTR agent's DegradedSubsystems payload so
// meshmon / the platform status page can deserialise either source with
// one struct (see platform/observability/health.Entry docs). GeneratedAt
// is the server-side capture time — clients should treat any missing or
// zero-length Entries as a healthy platform, not a stale response.
type SubsystemsResponse struct {
	GeneratedAt time.Time      `json:"generatedAt"`
	Count       int            `json:"count"`
	Entries     []health.Entry `json:"entries"`
}

// SubsystemsHandler returns an http.HandlerFunc that renders the
// current health.Registry snapshot as JSON. It is intended for the
// /api/monitoring/subsystems surface on help.orbtr.io / node.hstles.com
// / monitoring.hstles.com — the same surface meshmon polls for
// /mesh-topology today. Zero degraded subsystems returns
// {count:0, entries:[]}, not a 204, so clients can treat every 2xx as
// "reachable and healthy" and every non-2xx as "endpoint down".
//
// Returns nil (which chi treats as "no handler mounted") if the
// runtime has no registry — a Runtime built via Initialize always has
// one, so the guard is here only for the zero-Runtime test path.
func (rt *Runtime) SubsystemsHandler() http.HandlerFunc {
	if rt == nil || rt.healthRegistry == nil {
		return nil
	}
	reg := rt.healthRegistry
	return func(w http.ResponseWriter, r *http.Request) {
		// CORS parity with /mesh-status so help.orbtr.io can fetch the
		// endpoint from the browser.
		origin := r.Header.Get("Origin")
		if origin == "https://help.orbtr.io" || origin == "https://localhost:8080" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		snap := reg.Snapshot()
		resp := SubsystemsResponse{
			GeneratedAt: snap.GeneratedAt,
			Count:       len(snap.Entries),
			Entries:     snap.Entries,
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache, no-store")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
