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
// Entries are obshealth.Entry values, whose JSON tags match the
// DegradedSubsystem shape other producers emit, so a monitoring client can
// decode either source with one struct. GeneratedAt is the server-side capture
// time — clients should treat any missing or zero-length Entries as a
// healthy node, not a stale response.
type SubsystemsResponse struct {
	GeneratedAt time.Time      `json:"generatedAt"`
	Count       int            `json:"count"`
	Entries     []health.Entry `json:"entries"`
}

// SubsystemsHandler returns an http.HandlerFunc that renders the
// current health.Registry snapshot as JSON. A composition root mounts it
// at /api/monitoring/subsystems (e.g. https://node.example.com/api/monitoring/subsystems),
// beside the /mesh-topology surface a monitoring client polls. Zero
// degraded subsystems returns {count:0, entries:[]}, not a 204, so
// clients can treat every 2xx as "reachable and healthy" and every
// non-2xx as "endpoint down".
//
// CORS: the request's Origin is echoed in Access-Control-Allow-Origin
// only when subsystemsOriginAllowed admits it against
// Config.SubsystemsAllowedOrigins, read once when this handler is built.
//
// Returns nil (which chi treats as "no handler mounted") if the
// runtime has no registry — a Runtime built via Initialize always has
// one, so the guard is here only for the zero-Runtime test path.
func (rt *Runtime) SubsystemsHandler() http.HandlerFunc {
	if rt == nil || rt.healthRegistry == nil {
		return nil
	}
	reg := rt.healthRegistry
	// Copy the configured list so a caller mutating its Config slice after
	// Initialize cannot race the request path. nil stays nil: it selects the
	// built-in origins, while a non-nil empty slice admits none.
	var allowedOrigins []string
	if configured := rt.cfg.SubsystemsAllowedOrigins; configured != nil {
		allowedOrigins = make([]string, len(configured))
		copy(allowedOrigins, configured)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// CORS: the same default origins and the same replace-on-configure
		// rule as meshstatus.NewHandler with WithAllowedOrigins.
		origin := r.Header.Get("Origin")
		if subsystemsOriginAllowed(origin, allowedOrigins) {
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

// subsystemsOriginAllowed reports whether SubsystemsHandler echoes origin in
// Access-Control-Allow-Origin. configured is Config.SubsystemsAllowedOrigins:
// nil admits the built-in pair below; a non-nil slice replaces that pair
// exactly. An empty origin (a request with no Origin header) is never
// echoed, and an empty entry in configured never matches.
func subsystemsOriginAllowed(origin string, configured []string) bool {
	if origin == "" {
		return false
	}
	if configured == nil {
		return origin == "https://help.orbtr.io" || origin == "https://localhost:8080"
	}
	for _, allowed := range configured {
		if allowed != "" && allowed == origin {
			return true
		}
	}
	return false
}
