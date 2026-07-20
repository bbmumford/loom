/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package capabilities

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// RoleSelectionMethod indicates how roles were determined
type RoleSelectionMethod string

const (
	// RoleSelectionMethodOverride means roles were explicitly set via ROLES env var
	RoleSelectionMethodOverride RoleSelectionMethod = "override"

	// RoleSelectionMethodAuto means roles were auto-selected based on capabilities/resources
	RoleSelectionMethodAuto RoleSelectionMethod = "auto"
)

// RoleSelectionResult contains the selected roles and metadata about how they were chosen
type RoleSelectionResult struct {
	// Roles is the final list of roles this node should run
	Roles []string

	// Method indicates how the roles were determined
	Method RoleSelectionMethod

	// Capabilities is the detected capability information (may be nil for override)
	Capabilities *Capabilities

	// Resources is the detected system resources (may be nil for override)
	Resources *SystemResources

	// Warnings contains any warnings generated during selection
	Warnings []string

	// OverrideActive indicates if ROLES env var was used
	OverrideActive bool
}

// roleSelectionRecorder is a callback for recording role selection metrics.
// Set by runtime via SetRoleSelectionRecorder to break the import cycle.
var roleSelectionRecorder func(result *RoleSelectionResult)

// SetRoleSelectionRecorder sets the callback for recording role selection metrics.
// Called by runtime during initialization to wire into MeshMetrics.
func SetRoleSelectionRecorder(fn func(result *RoleSelectionResult)) {
	roleSelectionRecorder = fn
}

// RecordRoleSelection records role selection metrics via the configured callback.
func RecordRoleSelection(result *RoleSelectionResult) {
	if roleSelectionRecorder != nil {
		roleSelectionRecorder(result)
	}
}

// SelectRoles determines which roles this node should run based on:
// 1. Explicit override (ROLES env var) - skips all checks, emergency use only
// 2. Auto-selection (capabilities + resources + health checks) - production default
//
// The config parameter defines the project-specific role definitions, valid
// role names, and priority ordering. Each project (HSTLES, ORBTR) provides
// its own RoleConfig.
func SelectRoles(config *RoleConfig) *RoleSelectionResult {
	result := &RoleSelectionResult{
		Warnings: []string{},
	}

	// Check for ROLES override
	rolesEnv := os.Getenv("ROLES")
	requestedRoles := parseAndValidateRoles(rolesEnv, config, result)

	if len(requestedRoles) > 0 {
		result.Method = RoleSelectionMethodOverride
		result.Roles = requestedRoles
		result.OverrideActive = true
		dbgCapabilities.Printf("ROLES override active: %v", requestedRoles)
		log.Printf("WARNING: Capability and resource gating skipped - ensure dependencies present")
		result.Warnings = append(result.Warnings,
			"ROLES override active - capability and resource gating skipped",
			"Ensure this node has required dependencies for specified roles",
		)

		// Override skips *gating* but still populates Capabilities + Resources
		// so downstream consumers that read Capabilities.Has("auth") /
		// Resources.MaxRoles get truthful answers. Without this, override
		// mode produces a half-initialised RoleSelectionResult and any
		// callsite gating partial initialisation on Capabilities.Has(role)
		// silently no-ops — for instance, the auth role's handler
		// registration was being skipped because authCapable derived from
		// nil Capabilities returned false despite "auth" being explicitly
		// selected. The override is meant to bypass *checks*, not data.
		result.Capabilities = DetectCapabilities(config)
		result.Resources = DetectSystemResources()

		RecordRoleSelection(result)
		return result
	}

	// Auto-selection based on capabilities and resources
	result.Method = RoleSelectionMethodAuto
	result.Capabilities = DetectCapabilities(config)
	result.Resources = DetectSystemResources()

	// Get list of capable roles (has required environment configuration)
	capableRoles := result.Capabilities.GetCapableRoles()

	if len(capableRoles) == 0 {
		log.Printf("WARNING: No roles capable - missing environment configuration")
		log.Printf("WARNING: Node will start but not register any service capabilities")
		result.Warnings = append(result.Warnings,
			"No roles capable - check environment configuration",
			"Node will start but not register any service capabilities",
		)
		RecordRoleSelection(result)
		return result
	}

	// All capable roles are eligible (no manual authorization required)
	eligibleRoles := capableRoles

	// Apply resource limits (prevent overloading small instances)
	maxRoles := result.Resources.MaxRoles
	if len(eligibleRoles) > maxRoles {
		dbgCapabilities.Printf("%d roles eligible, but resources limit to %d concurrent roles",
			len(eligibleRoles), maxRoles)

		// Use project-specific priority ordering
		selectedRoles := []string{}
		for _, role := range config.PriorityOrder {
			if Contains(eligibleRoles, role) {
				selectedRoles = append(selectedRoles, role)
				if len(selectedRoles) >= maxRoles {
					break
				}
			}
		}

		// Include any eligible roles not in priority list (lower priority)
		if len(selectedRoles) < maxRoles {
			for _, role := range eligibleRoles {
				if !Contains(selectedRoles, role) {
					selectedRoles = append(selectedRoles, role)
					if len(selectedRoles) >= maxRoles {
						break
					}
				}
			}
		}

		result.Roles = selectedRoles
		dbgCapabilities.Printf("Resource-limited to roles: %v", result.Roles)
	} else {
		result.Roles = eligibleRoles
	}

	dbgCapabilities.Printf("Selected roles: %v (capable: %v, max concurrent: %d)",
		result.Roles, capableRoles, maxRoles)

	RecordRoleSelection(result)
	return result
}

// parseAndValidateRoles parses a comma-separated role string and validates
// against the valid role names defined in the given RoleConfig.
func parseAndValidateRoles(rolesEnv string, config *RoleConfig, result *RoleSelectionResult) []string {
	if rolesEnv == "" {
		return []string{}
	}

	validNames := config.ValidRoleNames()

	// Parse comma-separated list
	parts := strings.Split(rolesEnv, ",")
	validRoles := []string{}

	for _, part := range parts {
		role := strings.TrimSpace(part)
		if role == "" {
			continue
		}

		// Validate against known roles
		if !Contains(validNames, role) {
			warning := fmt.Sprintf("Invalid role %q - valid roles: %v", role, validNames)
			log.Printf("WARNING: %s", warning)
			if result != nil {
				result.Warnings = append(result.Warnings, warning)
			}
			continue
		}

		validRoles = append(validRoles, role)
	}

	return validRoles
}

// Contains checks if a slice contains a string (exported for use by callers)
func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// LogSummary logs a detailed summary of the role selection process
func (r *RoleSelectionResult) LogSummary() {
	dbgCapabilities.Printf("═══════════════════════════════════════════════════════════════")
	dbgCapabilities.Printf("ROLE SELECTION SUMMARY")
	dbgCapabilities.Printf("═══════════════════════════════════════════════════════════════")

	// Method
	dbgCapabilities.Printf("Selection Method: %s", r.Method)
	if r.OverrideActive {
		dbgCapabilities.Printf("  ROLES override active - capability/resource checks skipped")
	}

	// Capabilities (if detected)
	if r.Capabilities != nil {
		dbgCapabilities.Printf("Capability Detection:")
		r.Capabilities.LogCapabilities()
	}

	// Resources (if detected)
	if r.Resources != nil {
		dbgCapabilities.Printf("System Resources:")
		dbgCapabilities.Printf("  Memory: %d MB", r.Resources.MemoryMB)
		dbgCapabilities.Printf("  CPU Cores: %d", r.Resources.CPUCores)
		dbgCapabilities.Printf("  Max Concurrent Roles: %d", r.Resources.MaxRoles)
		dbgCapabilities.Printf("  Current Capacity: %.1f%%", r.Resources.Capacity*100)
	}

	// Selected roles
	dbgCapabilities.Printf("Selected Roles:")
	if len(r.Roles) == 0 {
		dbgCapabilities.Printf("  No roles selected")
	} else {
		for _, role := range r.Roles {
			dbgCapabilities.Printf("  %s", role)
		}
	}

	// Warnings
	if len(r.Warnings) > 0 {
		dbgCapabilities.Printf("Warnings:")
		for _, warning := range r.Warnings {
			dbgCapabilities.Printf("  %s", warning)
		}
	}

	dbgCapabilities.Printf("═══════════════════════════════════════════════════════════════")
}
