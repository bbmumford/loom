/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sort"

	"github.com/bbmumford/loom/pkg/rpc"
)

// DirectoryEntry describes one registered handler for the
// `/rpc/_directory` introspection endpoint.
//
// The fields are the metadata the e2e probe generator needs to decide
// how to call a handler — its FQN, its tenant scope (so probes know
// what auth context to set up), its tags (so the destructive class
// can be skipped), and the Go type names of its request and response
// (which let the probe synthesise a JSON body that's at least
// shape-valid).
type DirectoryEntry struct {
	FQN          string   `json:"fqn"`
	Namespace    string   `json:"namespace"`
	Domain       string   `json:"domain"`
	Operation    string   `json:"operation"`
	Scope        string   `json:"scope,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	RequestType  string   `json:"requestType,omitempty"`  // Go type name
	ResponseType string   `json:"responseType,omitempty"` // Go type name
	IsProxy      bool     `json:"isProxy,omitempty"`      // true = forwarded over mesh
}

// DirectorySummary is the wire shape returned to UNAUTHENTICATED callers.
// It exposes only the role inventory + handler count — no FQN, no Scope,
// no Tags, no proxy markers. Enough for a public service-discovery view
// without giving anonymous callers a recon map of the API surface.
type DirectorySummary struct {
	Count int            `json:"count"`
	Roles map[string]int `json:"roles"` // role → handler count
}

// DirectoryResponse is the wire shape returned to AUTHENTICATED callers
// (and the verbose=true query path). Includes per-handler metadata.
type DirectoryResponse struct {
	Count   int              `json:"count"`
	Entries []DirectoryEntry `json:"entries"`
	Roles   map[string]int   `json:"roles"`
}

// DirectoryAuthCheck is the auth-decision hook for HandleDirectory.
// Returns true when the caller should see the FULL directory; false when
// they should see only the unauthenticated summary. Bridges that wire an
// auth-aware check pass it via Bridge.SetDirectoryAuth; the free
// HandleDirectory function accepts the predicate as a parameter. In both
// cases a nil predicate fail-closes to the summary — the safer default
// for a fresh registry whose auth wiring isn't yet decided.
//
// Related: Bridge.HandleDirectory (per-registry auth wiring),
// finding rev-080 (why we killed the package-global var).
type DirectoryAuthCheck = func(r *http.Request) bool

// HandleDirectory returns a JSON listing of registered handlers.
//
// Two response modes, gated by the auth predicate captured at
// construction time:
//   - UNAUTHENTICATED → DirectorySummary (count + role inventory only)
//   - AUTHENTICATED   → DirectoryResponse (full entries with FQN/Scope/Tags)
//
// auth == nil is treated as "always unauthenticated" — the safer default.
// The predicate is a plain function value captured in the returned
// closure; it cannot be swapped after construction, so no synchronization
// is required on the request path. Endpoints that need to rotate the
// predicate at runtime should use Bridge.SetDirectoryAuth +
// Bridge.HandleDirectory instead, which uses an atomic.Pointer.
//
// Per-registry auth (finding rev-080): the previous package-level
// var + SetDirectoryAuthCheck design meant a single process-wide
// auth predicate applied to every rpc.Registry mounted in the process.
// Two registries mounted in the same binary (e.g. a user-tier proxy
// registry alongside a control-plane registry) inherited the same
// bypass. The new signature takes the predicate as an argument, so
// each mount decides its own gating.
//
// The summary intentionally exposes no FQN, Scope, or proxy markers —
// anonymous callers see the SHAPE of the API surface (which roles exist,
// how many handlers each role has) but not WHICH endpoints exist or what
// auth they need. That's enough for public service-discovery views
// (status pages, health dashboards) without giving attackers a recon map.
//
// The endpoint is read-only and exposes only metadata (no payloads). It's
// safe to mount under wildcard /rpc/_directory once an auth check is
// wired; the legacy "safe to mount publicly" advice in older versions of
// this comment was incorrect for the verbose response.
func HandleDirectory(reg *rpc.Registry, auth DirectoryAuthCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeDirectory(w, r, reg, auth)
	}
}

// writeDirectory is the shared render body used by both the free
// HandleDirectory function and Bridge.HandleDirectory. Splitting it out
// keeps the two entry points in exact lockstep — they cannot diverge on
// summary/verbose shape or on the fail-closed default when auth is nil.
func writeDirectory(w http.ResponseWriter, r *http.Request, reg *rpc.Registry, auth DirectoryAuthCheck) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	all := reg.All()
	roles := make(map[string]int)
	for _, h := range all {
		roles[h.Role()]++
	}

	verboseAllowed := auth != nil && auth(r)
	if !verboseAllowed {
		_ = json.NewEncoder(w).Encode(DirectorySummary{
			Count: len(all),
			Roles: roles,
		})
		return
	}

	entries := make([]DirectoryEntry, 0, len(all))
	for _, h := range all {
		entries = append(entries, DirectoryEntry{
			FQN:          h.FQN(),
			Namespace:    h.Namespace,
			Domain:       h.Domain,
			Operation:    h.Operation,
			Scope:        string(h.Scope),
			Tags:         append([]string(nil), h.Tags...),
			RequestType:  typeName(h.Request),
			ResponseType: typeName(h.Response),
			IsProxy:      h.Func == nil,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].FQN < entries[j].FQN })

	_ = json.NewEncoder(w).Encode(DirectoryResponse{
		Count:   len(entries),
		Entries: entries,
		Roles:   roles,
	})
}

// typeName extracts the unqualified Go type name from a proto.Message
// type hint (e.g. `*pb.ValidateSessionRequest` → "ValidateSessionRequest").
// Returns "" if the hint is nil.
func typeName(v any) string {
	if v == nil {
		return ""
	}
	t := reflect.TypeOf(v)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}
