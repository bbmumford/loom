/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package meshstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/ports"
)

// Response is the standard response shape for GET /mesh/status.
type Response struct {
	NodeID      string     `json:"nodeId"`
	ServiceName string     `json:"serviceName"`
	Region      string     `json:"region"`
	Roles       []string   `json:"roles"`
	Uptime      string     `json:"uptime"`
	StartedAt   time.Time  `json:"startedAt"`
	Peers       []PeerInfo `json:"peers"`
	VL1         VL1Summary `json:"vl1"`
	LAD         LADSummary `json:"lad"`
}

// PeerInfo describes a connected mesh peer.
type PeerInfo struct {
	NodeID         string   `json:"nodeId"`
	ServiceName    string   `json:"serviceName,omitempty"` // peer's HTTP-routable hostname from LAD member attrs
	LatencyMs      int64    `json:"latencyMs"`
	Region         string   `json:"region,omitempty"`
	Roles          []string `json:"roles,omitempty"`
	ConnectionType string   `json:"connectionType"` // "direct" or "relayed"
	Availability   float64  `json:"availability"`
}

// VL1Summary exposes aggregate VL1 transport metrics.
type VL1Summary struct {
	ActiveSessions   int    `json:"activeSessions"`
	TotalConnections uint64 `json:"totalConnections"`
	FailedDials      uint64 `json:"failedDials"`
}

// LADSummary exposes aggregate LAD directory counts.
type LADSummary struct {
	KnownMembers int `json:"knownMembers"`
	KnownRoles   int `json:"knownRoles"`
}

// RuntimeProvider abstracts the mesh node runtime so the handler can be tested
// independently and avoids a circular import on the node package.
type RuntimeProvider interface {
	NodeID() aether.NodeID
	MeshMetrics() map[string]interface{}
	Directory() DirectoryProvider
	ServiceName() string
}

// DirectoryProvider abstracts the LAD directory cache.
type DirectoryProvider interface {
	Members(ctx context.Context, partition string) ([]lad.MemberRecord, error)
	Roles(ctx context.Context, partition string, query ladcache.RoleQuery) ([]lad.RoleRecord, error)
	Reach(ctx context.Context, partition string, query ladcache.ReachQuery) ([]lad.ReachRecord, error)
}

// Option configures NewHandler and NewHandlerFromRuntime.
type Option func(*handlerOptions)

// handlerOptions holds the settings Options write. allowedOrigins is nil
// until WithAllowedOrigins runs; nil selects the built-in origin in
// originAllowed.
type handlerOptions struct {
	allowedOrigins []string
}

// WithAllowedOrigins replaces the built-in CORS origin with exactly
// origins: the handler echoes a request's Origin header in
// Access-Control-Allow-Origin only when it equals one entry byte-for-byte
// (scheme://host[:port]). Calling it with no origins admits no cross-origin
// caller; empty entries never match. A composition root that serves a
// browser page reading /mesh/status passes that page's origin here. The
// slice is copied, so the caller may reuse it. The last WithAllowedOrigins
// passed wins.
func WithAllowedOrigins(origins ...string) Option {
	list := make([]string, len(origins))
	copy(list, origins)
	return func(o *handlerOptions) {
		o.allowedOrigins = list
	}
}

// originAllowed reports whether the handler echoes origin in
// Access-Control-Allow-Origin. configured is nil unless WithAllowedOrigins
// was passed: nil admits only the built-in origin https://localhost:8080; a
// non-nil slice replaces it exactly, so an empty one admits nothing. An empty
// origin (a request with no Origin header) is never echoed, and an empty
// entry in configured never matches.
func originAllowed(origin string, configured []string) bool {
	if origin == "" {
		return false
	}
	if configured == nil {
		return origin == "https://localhost:8080"
	}
	for _, allowed := range configured {
		if allowed != "" && allowed == origin {
			return true
		}
	}
	return false
}

// NewHandler returns an http.HandlerFunc that serves GET /mesh/status.
// It exposes public, aggregate mesh status data suitable for external monitoring.
// Intentionally does NOT expose: private keys, PSKs, partition IDs, IP addresses, handler names.
// opts are applied in order when the handler is built; with no
// WithAllowedOrigins, CORS admits only the built-in origin in originAllowed.
func NewHandler(provider RuntimeProvider, platform ports.PlatformInfo, startTime time.Time, opts ...Option) http.HandlerFunc {
	var settings handlerOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&settings)
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// CORS — echo the Origin only when the allow-list admits it, so a
		// browser page on an admitted origin can read this response.
		origin := r.Header.Get("Origin")
		if originAllowed(origin, settings.allowedOrigins) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		ctx := r.Context()
		nodeID := provider.NodeID()

		// Region from injected platform
		region := platform.Region()

		// Roles from environment
		var roles []string
		if rolesStr := os.Getenv("ROLES"); rolesStr != "" {
			roles = strings.Split(rolesStr, ",")
		}

		// VL1 metrics
		vl1Raw := provider.MeshMetrics()
		vl1 := VL1Summary{}
		if v, ok := vl1Raw["active_sessions"].(int); ok {
			vl1.ActiveSessions = v
		}
		if v, ok := vl1Raw["total_connections"].(uint64); ok {
			vl1.TotalConnections = v
		}
		if v, ok := vl1Raw["failed_dials"].(uint64); ok {
			vl1.FailedDials = v
		}

		// LAD summary & peer list
		ladSummary := LADSummary{}
		var peers []PeerInfo

		dir := provider.Directory()
		if dir != nil {
			// Members feed both the LAD count and the per-peer
			// serviceName lookup. Without the latter the topology
			// surface renders every peer as "(unresolved)" because
			// reach records don't carry the HTTP-routable hostname —
			// that lives in MemberRecord.Attrs["serviceName"].
			serviceMap := make(map[string]string)
			if members, err := dir.Members(ctx, ""); err == nil {
				ladSummary.KnownMembers = len(members)
				for _, m := range members {
					if svc := m.Attrs["serviceName"]; svc != "" {
						serviceMap[m.NodeID] = svc
					}
				}
			}

			// Collect roles (for peer enrichment)
			roleMap := make(map[string][]string) // nodeID → roles
			if roleRecords, err := dir.Roles(ctx, "", ladcache.RoleQuery{}); err == nil {
				ladSummary.KnownRoles = len(roleRecords)
				for _, rr := range roleRecords {
					roleMap[rr.NodeID] = rr.Roles
				}
			}

			// Build peer list from reach records
			if reachRecords, err := dir.Reach(ctx, "", ladcache.ReachQuery{}); err == nil {
				for _, rr := range reachRecords {
					// Skip self
					if rr.NodeID == string(nodeID) {
						continue
					}

					connType := "direct"
					// If the reach record has no public-scope addresses, it's likely relayed
					hasPublic := false
					for _, addr := range rr.Addresses {
						if addr.Scope == "public" {
							hasPublic = true
							break
						}
					}
					if !hasPublic && len(rr.Addresses) > 0 {
						connType = "relayed"
					}

					peers = append(peers, PeerInfo{
						NodeID:         rr.NodeID,
						ServiceName:    serviceMap[rr.NodeID],
						LatencyMs:      rr.LatencyMillis,
						Region:         rr.Region,
						Roles:          roleMap[rr.NodeID],
						ConnectionType: connType,
						Availability:   rr.Availability,
					})
				}
			}
		}

		resp := Response{
			NodeID:      string(nodeID),
			ServiceName: provider.ServiceName(),
			Region:      region,
			Roles:       roles,
			Uptime:      time.Since(startTime).Truncate(time.Second).String(),
			StartedAt:   startTime,
			Peers:       peers,
			VL1:         vl1,
			LAD:         ladSummary,
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache, no-store")
		json.NewEncoder(w).Encode(resp)
	}
}
