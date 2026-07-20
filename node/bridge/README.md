# Gateway Layer (DEPRECATED)

**Status**: ⚠️ **DEPRECATED** as of November 2025

---

## Deprecation Notice

The gateway role has been **removed from node.hstles.com**. All HTTP/gRPC gateway functionality is now handled by the dedicated **api.hstles.com** endpoint.

---

## Migration Path

### Old Architecture (Deprecated)
```
External Client → node.hstles.com (GATEWAY role) → RestBridge → Mesh RPC
```

### New Architecture (Current)
```
External Client → api.hstles.com → UnifiedClient → MeshClient → node.hstles.com (VL1 RPC)
```

---

## What Was Removed

1. **Gateway Role**: No longer registered in node.hstles.com
2. **RestBridge HTTP Server**: Replaced by domain HTTP routes in api.hstles.com
3. **Bridge Discovery**: Replaced by direct LAD discovery in MeshClient

---

## What Remains (Deprecated)

- `bridge/` - RestBridge code (kept temporarily for reference)
  - Will generate deprecation warnings if used
  - Scheduled for complete removal in Q2 2026

---

## Replacement Components

| Old (Deprecated) | New (Current) |
|------------------|---------------|
| RestBridge HTTP routes | `domain/*/api/generated/http.go` |
| Bridge discovery | MeshClient with LAD discovery |
| Gateway node | api.hstles.com (dedicated service) |

---

## Removal Timeline

- **November 2025**: Marked deprecated, warnings added
- **Q1 2026**: Deprecation warnings in all generated code
- **Q2 2026**: Complete removal from codebase

---

## For New Development

❌ **Do NOT use**:
- `library/mesh/gateway/bridge/`
- RestBridge HTTP routes
- Gateway role in node.hstles.com

✅ **Use instead**:
- Domain HTTP routes in `library/domain/*/api/generated/http.go`
- MeshClient for mesh communication
- api.hstles.com for external HTTP/gRPC

---

## See Also

- [MESH-RESTRUCTURE-AND-IMPLEMENTATION-PLAN.md](../docs/MESH-RESTRUCTURE-AND-IMPLEMENTATION-PLAN.md) - Phase A8
- api.hstles.com documentation
- UnifiedClient documentation
