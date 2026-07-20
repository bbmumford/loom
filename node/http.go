/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
)

// SnapshotHandler returns an HTTP handler that serves the latest anchor snapshot.
func (rt *Runtime) SnapshotHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Rate Limiting (Token Bucket)
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		// Handle X-Forwarded-For if behind proxy
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			ip = strings.TrimSpace(strings.Split(fwd, ",")[0])
		}

		if !rt.checkSnapshotRateLimit(ip) {
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
