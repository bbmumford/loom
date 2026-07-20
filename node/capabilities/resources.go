/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package capabilities

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// SystemResources represents available system resources
type SystemResources struct {
	MemoryMB int64   // Available memory in megabytes
	CPUCores int     // Number of CPU cores
	MaxRoles int     // Maximum concurrent roles based on resources
	Capacity float64 // Current system capacity (0.0-1.0, higher = more available)
}

// DetectSystemResources probes the system to determine available resources
// and calculates the maximum number of roles that can run concurrently
func DetectSystemResources() *SystemResources {
	resources := &SystemResources{
		Capacity: 0.8, // Default capacity
	}

	// Detect memory
	resources.MemoryMB = detectMemory()

	// Detect CPU cores
	resources.CPUCores = detectCPU()

	// Calculate max concurrent roles based on resources
	resources.MaxRoles = calculateMaxRoles(resources.MemoryMB, resources.CPUCores)

	// Calculate current capacity (CPU + memory based)
	resources.Capacity = calculateCapacity()

	return resources
}

// detectMemory returns available memory in megabytes
// Tries multiple detection methods with fallbacks
func detectMemory() int64 {
	// Try gopsutil first (most accurate)
	if v, err := mem.VirtualMemory(); err == nil {
		return int64(v.Total / 1024 / 1024) // bytes to MB
	}

	// Fallback: check MEMORY_LIMIT_MB env var (useful in containers)
	if memLimit := os.Getenv("MEMORY_LIMIT_MB"); memLimit != "" {
		if mb, err := strconv.ParseInt(memLimit, 10, 64); err == nil {
			dbgCapabilities.Printf("Using MEMORY_LIMIT_MB from environment: %dMB", mb)
			return mb
		}
	}

	// Fallback: runtime.MemStats (less accurate, includes heap only)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	heapMB := int64(memStats.Sys / 1024 / 1024)
	dbgCapabilities.Printf("Warning: Using runtime heap size as memory estimate: %dMB", heapMB)
	return heapMB
}

// detectCPU returns the number of CPU cores
// Tries multiple detection methods with fallbacks
func detectCPU() int {
	// Try gopsutil first (most accurate, includes logical cores)
	if counts, err := cpu.Counts(true); err == nil && counts > 0 {
		return counts
	}

	// Fallback: check CPU_CORES env var (useful in containers)
	if cpuCores := os.Getenv("CPU_CORES"); cpuCores != "" {
		if cores, err := strconv.Atoi(cpuCores); err == nil && cores > 0 {
			dbgCapabilities.Printf("Using CPU_CORES from environment: %d", cores)
			return cores
		}
	}

	// Fallback: runtime.NumCPU (GOMAXPROCS value)
	cores := runtime.NumCPU()
	if cores <= 0 {
		cores = 1 // Absolute minimum
	}
	return cores
}

// calculateMaxRoles determines the maximum number of concurrent roles
// based on available memory and CPU cores
//
// Rules:
//   - <512MB, 1 CPU → max 1 role
//   - 512MB-1GB, 1-2 CPUs → max 2 roles
//   - ≥1GB, ≥2 CPUs → max 3 roles
//   - Can be overridden with MAX_ROLES env var
func calculateMaxRoles(memoryMB int64, cpuCores int) int {
	// Check for explicit override
	if maxRolesEnv := os.Getenv("MAX_ROLES"); maxRolesEnv != "" {
		if maxRoles, err := strconv.Atoi(maxRolesEnv); err == nil && maxRoles > 0 {
			dbgCapabilities.Printf("Using MAX_ROLES from environment: %d", maxRoles)
			return maxRoles
		}
	}

	// Resource-based calculation
	if memoryMB < 512 || cpuCores < 1 {
		return 1 // Minimal resources: 1 role max
	}

	if memoryMB < 1024 || cpuCores < 2 {
		return 2 // Moderate resources: 2 roles max
	}

	return 3 // Ample resources: 3 roles max
}

// calculateCapacity estimates current system capacity (0.0-1.0)
// Higher values indicate more available resources
// This is used for task bidding and load balancing
func calculateCapacity() float64 {
	// Try to get real-time metrics
	var cpuPercent float64 = 20.0 // Default: assume 20% CPU usage
	var memPercent float64 = 20.0 // Default: assume 20% memory usage

	// Get CPU usage
	if percentages, err := cpu.Percent(0, false); err == nil && len(percentages) > 0 {
		cpuPercent = percentages[0]
	}

	// Get memory usage
	if v, err := mem.VirtualMemory(); err == nil {
		memPercent = v.UsedPercent
	}

	// Capacity = 1.0 - average(cpu_percent, mem_percent) / 100
	avgUsage := (cpuPercent + memPercent) / 2.0
	capacity := 1.0 - (avgUsage / 100.0)

	// Clamp to [0.0, 1.0]
	if capacity < 0.0 {
		capacity = 0.0
	}
	if capacity > 1.0 {
		capacity = 1.0
	}

	return capacity
}

// LogResources logs detected system resources in a human-readable format
func (r *SystemResources) LogResources() {
	dbgCapabilities.Printf("System Resources Detected:")
	dbgCapabilities.Printf("  Memory:      %dMB", r.MemoryMB)
	dbgCapabilities.Printf("  CPU Cores:   %d", r.CPUCores)
	dbgCapabilities.Printf("  Max Roles:   %d concurrent", r.MaxRoles)
	dbgCapabilities.Printf("  Capacity:    %.1f%% available", r.Capacity*100)
}

// MeetsGatewayRequirements checks if system resources meet gateway role minimums
// Gateway requires at least 512MB RAM and 1 CPU core
func (r *SystemResources) MeetsGatewayRequirements() bool {
	return r.MemoryMB >= 512 && r.CPUCores >= 1
}

// CanRunAdditionalRole checks if system resources allow running another role
// based on the current number of active roles
func (r *SystemResources) CanRunAdditionalRole(currentRoleCount int) bool {
	return currentRoleCount < r.MaxRoles
}

// GetResourceTier returns a human-readable tier classification
func (r *SystemResources) GetResourceTier() string {
	if r.MemoryMB >= 1024 && r.CPUCores >= 2 {
		return "high" // Can run 3+ roles
	}
	if r.MemoryMB >= 512 && r.CPUCores >= 1 {
		return "medium" // Can run 2 roles
	}
	return "low" // Can run 1 role max
}

// GetRecommendedRoles returns recommended role count based on resources
func (r *SystemResources) GetRecommendedRoles() int {
	// Conservative: return one less than max to leave headroom
	recommended := r.MaxRoles - 1
	if recommended < 1 {
		recommended = 1
	}
	return recommended
}

// ParseResourceString parses a resource string like "512MB" or "2cores" into components
// Useful for parsing container resource limits
func ParseResourceString(resourceStr string) (value int64, unit string) {
	resourceStr = strings.ToLower(strings.TrimSpace(resourceStr))

	// Extract numeric part
	var numStr strings.Builder
	var unitStr strings.Builder
	inNumber := true

	for _, char := range resourceStr {
		if inNumber && (char >= '0' && char <= '9' || char == '.') {
			numStr.WriteRune(char)
		} else {
			inNumber = false
			unitStr.WriteRune(char)
		}
	}

	// Parse value
	if num, err := strconv.ParseFloat(numStr.String(), 64); err == nil {
		value = int64(num)
	}

	unit = unitStr.String()
	return
}
