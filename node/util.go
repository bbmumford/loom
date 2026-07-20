/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

// TruncateNodeID returns the first n characters of a node ID for logging.
// If the ID is shorter than n, it is returned as-is.
func TruncateNodeID(id string, n int) string {
	if len(id) > n {
		return id[:n]
	}
	return id
}

// truncID returns first 12 chars of a node ID plus "..." for logging.
func truncID(id string) string {
	if len(id) > 12 {
		return id[:12] + "..."
	}
	return id
}
