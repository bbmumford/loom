# Debug Logging Package

Centralized, flag-deterministic debug logging for HSTLES platform.

## Features

- **Flag-deterministic**: Controlled via `DEBUG` environment variable or `Configure()` call
- **Namespaced**: Debug logs tagged by component (e.g., `mesh.vl1`, `mesh.lad`, `endpoint.login`)
- **Zero overhead when disabled**: No-op when debug flag is false
- **Thread-safe**: Concurrent logging from multiple goroutines
- **Flexible output**: Configurable output destination (default: stderr)
- **Performance tracking**: Built-in benchmark and trace helpers

## Quick Start

### Environment Variable

```bash
# Enable debug globally
DEBUG=1 go run main.go

# Disable debug (default)
DEBUG=0 go run main.go
# or
unset DEBUG
```

### Programmatic Configuration

```go
import "github.com/hstles/library/platform/debug"

// Enable debug logging
debug.SetEnabled(true)

// Or configure with custom output
debug.Configure(true, os.Stdout)

// Check if debug is enabled
if debug.IsEnabled() {
    // ...
}
```

## Usage

### Basic Logging

```go
package mypackage

import "github.com/hstles/library/platform/debug"

var dbg = debug.New("mesh.vl1")

func MyFunction() {
    dbg.Printf("Processing request: %s", requestID)
    dbg.Println("Operation completed")
}
```

### Namespace Examples

- `mesh.vl1` - VL1 overlay transport
- `mesh.lad` - LAD directory
- `mesh.rpc` - RPC executor
- `mesh.task` - Task executor
- `mesh.relay` - Relay fallback
- `endpoint.login` - Login endpoint
- `endpoint.api` - API endpoint
- `handler.auth` - Auth handlers

### Performance Tracking

```go
var dbg = debug.New("mesh.vl1")

func ExpensiveOperation() {
    start := time.Now()
    defer dbg.Benchmark("ExpensiveOperation", start)
    
    // ... operation code ...
}
```

### Function Tracing

```go
var dbg = debug.New("mesh.lad")

func ComplexFlow() {
    defer dbg.Trace("ComplexFlow")()
    
    // Logs:
    // [TRACE] → ComplexFlow
    // ... function body ...
    // [TRACE] ← ComplexFlow (duration: 123ms)
}
```

## Output Format

```
2026/01/07 22:15:30.123456 [DEBUG:mesh.vl1] Connection established to node abc123
2026/01/07 22:15:30.456789 [DEBUG:mesh.rpc] Executing handler login.auth (request_id=xyz789)
2026/01/07 22:15:31.111111 [DEBUG:mesh.vl1] [BENCHMARK] Dial took 1.234s
2026/01/07 22:15:31.222222 [DEBUG:mesh.lad] [TRACE] → SyncDirectory
2026/01/07 22:15:31.333333 [DEBUG:mesh.lad] [TRACE] ← SyncDirectory (duration: 111ms)
```

## Integration Examples

### Mesh Node Initialization

```go
import (
    "github.com/hstles/library/platform/debug"
    "github.com/hstles/library/mesh/node"
)

func initMeshNode(cfg node.Config) (*node.Runtime, error) {
    // Enable debug if configured
    debug.SetEnabled(cfg.DebugEnabled)
    
    dbg := debug.New("mesh.node")
    dbg.Printf("Initializing mesh node: %s", cfg.ServiceName)
    
    runtime, err := node.Initialize(cfg)
    if err != nil {
        return nil, err
    }
    
    dbg.Printf("Mesh node ready: %s", runtime.NodeID())
    return runtime, nil
}
```

### Endpoint Initialization

```go
import "github.com/hstles/library/platform/debug"

func main() {
    // Load config
    var cfg Config
    // ...
    
    // Configure debug from app config
    debug.SetEnabled(cfg.App.LogLevel == "debug")
    
    dbg := debug.New("endpoint.login")
    dbg.Printf("Starting endpoint: %s", cfg.App.Name)
    
    // ... rest of initialization ...
}
```

## Advanced Features

### Per-Namespace Control

```go
// Enable debug globally
debug.SetEnabled(true)

// But disable for specific namespace
debug.SetNamespaceEnabled("mesh.lad", false)

// mesh.vl1 will log, mesh.lad won't
```

### Debug Information

```go
// Get all registered namespaces
namespaces := debug.GetNamespaces()

// Get debug summary
info := debug.DebugInfo()
fmt.Println(info)
// Output:
// Debug enabled: true
// Registered namespaces: 5
//   - mesh.vl1: enabled=true
//   - mesh.lad: enabled=true
//   - mesh.rpc: enabled=true
//   - endpoint.login: enabled=true
//   - handler.auth: enabled=true
```

## Best Practices

1. **Create logger once per package**: Use package-level variable
   ```go
   var dbg = debug.New("mesh.vl1")
   ```

2. **Use descriptive namespaces**: Follow pattern `component.subcomponent`
   ```go
   debug.New("mesh.vl1.relay")
   debug.New("mesh.rpc.executor")
   ```

3. **Don't use for production logs**: Debug is for development/troubleshooting
   - Use `log.Printf()` for production info/warning/error
   - Use `debug.Printf()` for verbose debugging

4. **Performance-sensitive code**: Debug check is fast but not free
   ```go
   // Good: minimal overhead
   dbg.Printf("result: %v", result)
   
   // Better for expensive formatting:
   if debug.IsEnabled() {
       expensiveData := computeExpensiveDebugInfo()
       dbg.Printf("data: %v", expensiveData)
   }
   ```

## Migration Guide

### From Standard log.Printf

```go
// Before
log.Printf("[Relay Fallback] NAT detection failed: %v", err)

// After
var dbg = debug.New("mesh.vl1.relay")
dbg.Printf("NAT detection failed: %v", err)
```

### From Conditional Logging

```go
// Before
if os.Getenv("DEBUG") != "" {
    log.Printf("Debug info: %v", data)
}

// After
var dbg = debug.New("mypackage")
dbg.Printf("Debug info: %v", data)
```

## Testing

Run tests:
```bash
cd Library/platform/debug
go test -v
go test -bench=. -benchmem
```

## Performance

Benchmark results (disabled logger):
- `BenchmarkLoggerDisabled`: ~0.5 ns/op, 0 B/op (single branch check)

Benchmark results (enabled logger):
- `BenchmarkLoggerEnabled`: ~500 ns/op, includes formatting and I/O
