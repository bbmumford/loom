/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
)

// RoleSelectionMetrics tracks metrics about role selection decisions
// In production, these would integrate with Prometheus/OpenTelemetry
type RoleSelectionMetrics struct {
	mu sync.RWMutex

	// SelectionMethod tracks how roles were selected (override | auto)
	SelectionMethod string

	// OverrideActive indicates if ROLES env var was used
	OverrideActive bool

	// RequestedRoles lists all roles that were requested (via ROLES override)
	RequestedRoles []string

	// SelectedRoles lists all roles that were actually selected
	SelectedRoles []string

	// CapableRoles lists all roles this node is capable of running
	CapableRoles []string

	// ResourceLimited indicates if role selection was limited by resources
	ResourceLimited bool

	// WarningCount is the number of warnings generated
	WarningCount int
}

var (
	// GlobalMetrics holds the current role selection metrics
	// In production, this would export to Prometheus/OTel
	GlobalMetrics = &RoleSelectionMetrics{}
)

// RecordRoleSelection records metrics from a role selection result
// RecordRoleSelection records metrics from a role selection result.
// Accepts minimal interface to avoid import cycles.
type minimalRoleSelectionResult interface {
	GetMethod() string
	IsOverrideActive() bool
	GetRoles() []string
	GetWarnings() []string
	GetCapabilities() interface{ GetCapableRoles() []string }
	GetResources() interface{}
}

func RecordRoleSelection(result minimalRoleSelectionResult) {
	if result == nil {
		return
	}

	GlobalMetrics.mu.Lock()
	defer GlobalMetrics.mu.Unlock()

	GlobalMetrics.SelectionMethod = result.GetMethod()
	GlobalMetrics.OverrideActive = result.IsOverrideActive()
	GlobalMetrics.SelectedRoles = result.GetRoles()
	GlobalMetrics.WarningCount = len(result.GetWarnings())

	if caps := result.GetCapabilities(); caps != nil {
		GlobalMetrics.CapableRoles = caps.GetCapableRoles()
	}

	if result.GetResources() != nil {
		GlobalMetrics.ResourceLimited = len(GlobalMetrics.SelectedRoles) < len(GlobalMetrics.CapableRoles)
	}

	// Log metrics (in production, export to Prometheus)
	logMetrics()
}

// logMetrics logs current metrics in a structured format
// In production, this would be replaced with Prometheus metric exports:
//   - role_selection_method{method="override|auto"} gauge
//   - role_override_active{} gauge (1 if override, 0 otherwise)
//   - role_requested{role="auth|identity|notify|gateway|maintenance"} gauge
//   - role_selected{role="auth|identity|notify|gateway|maintenance"} gauge
//   - role_capable{role="auth|identity|notify|gateway|maintenance"} gauge
//   - role_selection_warnings_total counter
func logMetrics() {
	dbgNodeMetrics.Printf("role_selection_method=%s", GlobalMetrics.SelectionMethod)
	dbgNodeMetrics.Printf("role_override_active=%t", GlobalMetrics.OverrideActive)
	dbgNodeMetrics.Printf("roles_selected=%d", len(GlobalMetrics.SelectedRoles))
	dbgNodeMetrics.Printf("roles_capable=%d", len(GlobalMetrics.CapableRoles))
	dbgNodeMetrics.Printf("resource_limited=%t", GlobalMetrics.ResourceLimited)
	dbgNodeMetrics.Printf("selection_warnings=%d", GlobalMetrics.WarningCount)

	for _, role := range GlobalMetrics.SelectedRoles {
		dbgNodeMetrics.Printf("role_selected{role=%q}=1", role)
	}

	for _, role := range GlobalMetrics.CapableRoles {
		dbgNodeMetrics.Printf("role_capable{role=%q}=1", role)
	}
}

// GetMetrics returns a read-only copy of current metrics
func GetMetrics() RoleSelectionMetrics {
	GlobalMetrics.mu.RLock()
	defer GlobalMetrics.mu.RUnlock()

	// Return copy to prevent external modification
	return RoleSelectionMetrics{
		SelectionMethod: GlobalMetrics.SelectionMethod,
		OverrideActive:  GlobalMetrics.OverrideActive,
		RequestedRoles:  append([]string(nil), GlobalMetrics.RequestedRoles...),
		SelectedRoles:   append([]string(nil), GlobalMetrics.SelectedRoles...),
		CapableRoles:    append([]string(nil), GlobalMetrics.CapableRoles...),
		ResourceLimited: GlobalMetrics.ResourceLimited,
		WarningCount:    GlobalMetrics.WarningCount,
	}
}

// ResetMetrics clears all metrics (primarily for testing)
func ResetMetrics() {
	GlobalMetrics.mu.Lock()
	defer GlobalMetrics.mu.Unlock()

	GlobalMetrics.SelectionMethod = ""
	GlobalMetrics.OverrideActive = false
	GlobalMetrics.RequestedRoles = nil
	GlobalMetrics.SelectedRoles = nil
	GlobalMetrics.CapableRoles = nil
	GlobalMetrics.ResourceLimited = false
	GlobalMetrics.WarningCount = 0
}
