/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"encoding/json"
	"net/http"

	"github.com/bbmumford/weft"
)

// SnapshotHandler returns an HTTP handler that serves the latest anchor snapshot.
func (rt *Runtime) SnapshotHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Rate Limiting (Token Bucket), keyed on the client address the
		// connection or the trusted edge proved.
		if !rt.checkSnapshotRateLimit(weft.DefaultClientIP(r)) {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		// 2. Authentication (Network Key HMAC)
		// Requires possession of the shared Network Key to access topology
		authHeader := r.Header.Get("X-Mesh-Auth")
		if authHeader == "" {
			http.Error(w, "Unauthorized: Missing auth token", http.StatusUnauthorized)
			return
		}

		if !rt.verifyAuthToken(authHeader) {
			http.Error(w, "Unauthorized: Invalid auth token", http.StatusUnauthorized)
			return
		}

		snapshot := rt.GetLatestSnapshot()
		if snapshot == nil {
			http.Error(w, "Snapshot not available", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(snapshot); err != nil {
			http.Error(w, "Failed to encode snapshot", http.StatusInternalServerError)
		}
	}
}
