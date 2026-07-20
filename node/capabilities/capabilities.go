/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package capabilities

import (
	"fmt"
	"sort"

	"github.com/bbmumford/cfgload"
)

// RoleRequirement defines the configuration dependencies for a specific role
type RoleRequirement struct {
	Name        string   // Role name (e.g., "auth", "identity")
	Description string   // Human-readable description of what the role does
	Configs     []string // Required config names in the registry (e.g., "auth", "smtp")
	Summary     string   // Summary format for success message
}

// RoleConfig defines the complete role configuration for a project.
// Each project (HSTLES, ORBTR) builds one of these and passes it to
// SelectRoles() and DetectCapabilities().
type RoleConfig struct {
	// Roles maps role name → requirement. Defines which roles this project
	// supports and what configuration each role needs.
	Roles map[string]RoleRequirement

	// PriorityOrder determines which roles to keep when resource limits
	// force a subset. First entry = highest priority. Roles not listed
	// are lower priority than all listed roles.
	PriorityOrder []string
}

// ValidRoleNames returns the sorted list of valid role names from the config.
func (rc *RoleConfig) ValidRoleNames() []string {
	names := make([]string, 0, len(rc.Roles))
	for name := range rc.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Capabilities represents what roles a node is capable of running
// based on available configuration, dependencies, and resources.
// Uses a dynamic map instead of hardcoded boolean fields so any
// project can define its own roles.
type Capabilities struct {
	Detected map[string]bool   // role name → capable (true/false)
	Reasons  map[string]string // role name → human-readable reason
}

// Has checks if a specific role capability is detected.
func (c *Capabilities) Has(role string) bool {
	return c.Detected[role]
}

// GetCapableRoles returns a list of role names this node can run
func (c *Capabilities) GetCapableRoles() []string {
	roles := []string{}
	for role, capable := range c.Detected {
		if capable {
			roles = append(roles, role)
		}
	}
	return roles
}

// HasAnyCapability returns true if the node can run at least one role
func (c *Capabilities) HasAnyCapability() bool {
	for _, capable := range c.Detected {
		if capable {
			return true
		}
	}
	return false
}

// LogCapabilities logs detected capabilities in a human-readable format
func (c *Capabilities) LogCapabilities() {
	dbgCapabilities.Printf("Role Capability Detection:")
	// Sort keys for consistent output
	roles := make([]string, 0, len(c.Reasons))
	for role := range c.Reasons {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		dbgCapabilities.Printf("  %-16s %s", role+":", c.Reasons[role])
	}
}

// DetectCapabilities analyzes the environment to determine which roles
// this node can run, based on the role definitions in the given config.
//
// This function performs capability detection WITHOUT initializing any services.
// It checks the cfgload registry for required configuration objects.
func DetectCapabilities(config *RoleConfig) *Capabilities {
	caps := &Capabilities{
		Detected: make(map[string]bool),
		Reasons:  make(map[string]string),
	}

	for roleName := range config.Roles {
		caps.Detected[roleName] = detectRoleCapability(roleName, config, &caps.Reasons)
	}

	return caps
}

// detectRoleCapability checks if ALL required dependencies for a role are configured.
// It checks the global configuration registry to see if the required configuration
// objects have been successfully loaded and registered.
func detectRoleCapability(roleName string, config *RoleConfig, reasons *map[string]string) bool {
	requirement, exists := config.Roles[roleName]
	if !exists {
		(*reasons)[roleName] = "Unknown role"
		return false
	}

	// Check each required config in the registry
	missing := []string{}
	for _, configName := range requirement.Configs {
		if !cfgload.Exists(configName) {
			missing = append(missing, configName)
		}
	}

	// If any required configs are missing, mark as not capable
	if len(missing) > 0 {
		(*reasons)[roleName] = fmt.Sprintf("Missing configs: %v", missing)
		return false
	}

	(*reasons)[roleName] = "✓ " + requirement.Summary
	return true
}
