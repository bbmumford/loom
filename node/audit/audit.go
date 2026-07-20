/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package audit

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// EventCategory defines the type of audit event
type EventCategory string

const (
	EventCategoryRaft     EventCategory = "RAFT"
	EventCategoryVL1      EventCategory = "VL1"
	EventCategorySecurity EventCategory = "SECURITY"
)

// AuditLogger handles security and operational event logging
type AuditLogger struct {
	logPath string
	enabled bool
	mu      sync.Mutex
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(logPath string, enabled bool) (*AuditLogger, error) {
	if !enabled {
		return &AuditLogger{
			enabled: false,
		}, nil
	}

	if logPath == "" {
		logPath = "./logs/mesh-audit.log"
	}

	// Ensure log directory exists
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create audit log directory: %w", err)
	}

	return &AuditLogger{
		logPath: logPath,
		enabled: true,
	}, nil
}

// LogRaftEvent logs a Raft-related event
func (a *AuditLogger) LogRaftEvent(eventType, details string) {
	if !a.enabled {
		return
	}

	msg := fmt.Sprintf("[AUDIT] [RAFT] %s | %s | %s\n",
		time.Now().Format(time.RFC3339),
		eventType,
		details)

	a.writeLog(msg)
}

// LogVL1Event logs a VL1 transport event
func (a *AuditLogger) LogVL1Event(eventType, nodeID, details string) {
	if !a.enabled {
		return
	}

	msg := fmt.Sprintf("[AUDIT] [VL1] %s | %s | node=%s | %s\n",
		time.Now().Format(time.RFC3339),
		eventType,
		nodeID,
		details)

	a.writeLog(msg)
}

// LogSecurityEvent logs a security-related event
func (a *AuditLogger) LogSecurityEvent(eventType, details string) {
	if !a.enabled {
		return
	}

	msg := fmt.Sprintf("[AUDIT] [SECURITY] %s | %s | %s\n",
		time.Now().Format(time.RFC3339),
		eventType,
		details)

	a.writeLog(msg)

	// Also log to standard logger for security events
	log.Printf("[SECURITY] %s - %s", eventType, details)
}

// LogEvent logs a generic event with category
func (a *AuditLogger) LogEvent(category EventCategory, eventType, details string) {
	if !a.enabled {
		return
	}

	msg := fmt.Sprintf("[AUDIT] [%s] %s | %s | %s\n",
		category,
		time.Now().Format(time.RFC3339),
		eventType,
		details)

	a.writeLog(msg)

	// Log security events to stdout as well
	if category == EventCategorySecurity {
		log.Printf("[SECURITY] %s - %s", eventType, details)
	}
}

// writeLog writes a message to the audit log file
func (a *AuditLogger) writeLog(msg string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Open log file for append
	f, err := os.OpenFile(a.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[AUDIT] Failed to open audit log: %v", err)
		return
	}
	defer f.Close()

	if _, err := f.WriteString(msg); err != nil {
		log.Printf("[AUDIT] Failed to write audit log: %v", err)
	}
}

// Close flushes and closes the audit logger
func (a *AuditLogger) Close() error {
	// Nothing to close since we open/close file on each write
	// This method exists for interface compatibility
	return nil
}

// IsEnabled returns whether audit logging is enabled
func (a *AuditLogger) IsEnabled() bool {
	return a.enabled
}

// Path returns the audit log file path
func (a *AuditLogger) Path() string {
	return a.logPath
}
