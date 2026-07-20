/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package debug

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDebugLogger(t *testing.T) {
	// Save original state
	originalEnabled := forceAllEnabled.Load()
	defer func() {
		forceAllEnabled.Store(originalEnabled)
	}()
	
	t.Run("disabled by default without env var", func(t *testing.T) {
		os.Unsetenv("DEBUG")
		forceAllEnabled.Store(false)
		
		var buf bytes.Buffer
		Configure(false, &buf)
		
		logger := New("test")
		logger.Printf("test message")
		
		if buf.Len() > 0 {
			t.Errorf("expected no output when disabled, got: %s", buf.String())
		}
	})
	
	t.Run("enabled when configured", func(t *testing.T) {
		var buf bytes.Buffer
		Configure(true, &buf)
		
		logger := New("test.namespace")
		logger.Printf("test message: %s", "value")
		
		output := buf.String()
		if !strings.Contains(output, "[DEBUG:test.namespace]") {
			t.Errorf("expected namespace in output, got: %s", output)
		}
		if !strings.Contains(output, "test message: value") {
			t.Errorf("expected message in output, got: %s", output)
		}
	})
	
	t.Run("namespace caching", func(t *testing.T) {
		Configure(true, nil)
		
		logger1 := New("cached.namespace")
		logger2 := New("cached.namespace")
		
		if logger1 != logger2 {
			t.Error("expected same logger instance for same namespace")
		}
	})
	
	t.Run("multiple namespaces", func(t *testing.T) {
		var buf bytes.Buffer
		Configure(true, &buf)
		
		logger1 := New("ns1")
		logger2 := New("ns2")
		
		logger1.Printf("message 1")
		logger2.Printf("message 2")
		
		output := buf.String()
		if !strings.Contains(output, "[DEBUG:ns1]") {
			t.Error("expected ns1 in output")
		}
		if !strings.Contains(output, "[DEBUG:ns2]") {
			t.Error("expected ns2 in output")
		}
	})
	
	t.Run("println method", func(t *testing.T) {
		var buf bytes.Buffer
		Configure(true, &buf)
		
		logger := New("println.test")
		logger.Println("message", "with", "args")
		
		output := buf.String()
		// fmt.Sprint doesn't add spaces between args without explicit formatting
		if !strings.Contains(output, "[DEBUG:println.test]") {
			t.Errorf("expected namespace in output, got: %s", output)
		}
		if !strings.Contains(output, "message") {
			t.Errorf("expected message in output, got: %s", output)
		}
	})
}

func TestIsEnabled(t *testing.T) {
	originalEnabled := forceAllEnabled.Load()
	defer func() {
		forceAllEnabled.Store(originalEnabled)
	}()
	
	SetEnabled(true)
	if !IsEnabled() {
		t.Error("expected IsEnabled to return true")
	}
	
	SetEnabled(false)
	if IsEnabled() {
		t.Error("expected IsEnabled to return false")
	}
}

func TestBenchmark(t *testing.T) {
	var buf bytes.Buffer
	Configure(true, &buf)
	
	logger := New("benchmark.test")
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	logger.Benchmark("test operation", start)
	
	output := buf.String()
	if !strings.Contains(output, "[BENCHMARK]") {
		t.Error("expected benchmark marker in output")
	}
	if !strings.Contains(output, "test operation") {
		t.Error("expected operation name in output")
	}
}

func TestTrace(t *testing.T) {
	var buf bytes.Buffer
	Configure(true, &buf)
	
	logger := New("trace.test")
	
	func() {
		defer logger.Trace("testFunction")()
		time.Sleep(5 * time.Millisecond)
	}()
	
	output := buf.String()
	if !strings.Contains(output, "[TRACE] → testFunction") {
		t.Error("expected trace entry in output")
	}
	if !strings.Contains(output, "[TRACE] ← testFunction") {
		t.Error("expected trace exit in output")
	}
	if !strings.Contains(output, "duration:") {
		t.Error("expected duration in trace output")
	}
}

func TestGetNamespaces(t *testing.T) {
	// Clear loggers
	mu.Lock()
	loggers = make(map[string]*Logger)
	mu.Unlock()
	
	Configure(true, nil)
	
	New("ns1")
	New("ns2")
	New("ns3")
	
	namespaces := GetNamespaces()
	if len(namespaces) != 3 {
		t.Errorf("expected 3 namespaces, got %d", len(namespaces))
	}
}

func TestDebugInfo(t *testing.T) {
	// Clear loggers
	mu.Lock()
	loggers = make(map[string]*Logger)
	mu.Unlock()
	
	Configure(true, nil)
	
	New("info.test1")
	New("info.test2")
	
	info := DebugInfo()
	// DebugInfo prints "Force-all enabled:" (the Configure(true,…) toggle);
	// the old "Debug enabled:" assertion predated that rename and also
	// fails against the original platform/debug package.
	if !strings.Contains(info, "Force-all enabled: true") {
		t.Error("expected force-all enabled in info")
	}
	if !strings.Contains(info, "info.test1") {
		t.Error("expected namespace info.test1 in info")
	}
	if !strings.Contains(info, "info.test2") {
		t.Error("expected namespace info.test2 in info")
	}
}

func BenchmarkLoggerDisabled(b *testing.B) {
	Configure(false, nil)
	logger := New("bench")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Printf("benchmark message %d", i)
	}
}

func BenchmarkLoggerEnabled(b *testing.B) {
	var buf bytes.Buffer
	Configure(true, &buf)
	logger := New("bench")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Printf("benchmark message %d", i)
	}
}
