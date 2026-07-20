/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"reflect"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
)

// Phase-0 golden surface test (plan Appendix A / §Gates Phase 0(b)).
//
// The baseline lists below were generated from the pre-extraction
// HSTLES/Library/mesh tree (branch fix/mesh-review-findings, 2026-07-19)
// by enumerating exported methods on the three load-bearing types. Every
// baseline symbol MUST remain present on the loom types — a missing symbol
// is a broken consumer. Additions are permitted (the extraction adds e.g.
// RPCServer.SetAuthValidator); removals are not.

var goldenRuntimeMethods = []string{
	"AcceptIncomingMeshSession", "AddDiscoverer", "AuditLogger",
	"BestGradeToHandler", "BootstrapHandler", "CircuitBreaker", "Config",
	"ConnectionEventLog", "ConnectionReporter", "ConnectivityTestHandler",
	"ConnManager", "Context", "Database", "DefaultTransport",
	"DialAndAcceptMesh", "DialByProtocol", "Directory", "GetLatestSnapshot",
	"GetPeerLatencies", "Go", "GoCtx", "GRPCServer", "HandleRelayFrame",
	"HasSession", "HealthCheck", "HealthEvaluator", "HealthRegistry",
	"HijackRelayHandler", "Identity", "IdentityPrivateKey", "InitRoute",
	"InitSwarm", "IsHealthy", "LADSnapshot", "LADSnapshotCache", "Ledger",
	"LocalLoad", "LookupRoleViaSwarm", "MeshDB", "MeshMetrics",
	"MeshSessionCount", "MetricsExporter", "NATBehaviour", "NATStrategy",
	"NodeID", "PeerDispatchHealth", "PickTraversalMethod",
	"PublishHandlersToLAD", "PublishRPCHandlersToLAD", "ReachLookupHandler",
	"RegisterExternalSession", "RegisterSession", "RegisterTenant",
	"Registry", "RemoveTenant", "Route", "RPCServer", "SelfHealth",
	"SessionMetrics", "SessionOptions", "SetHealthEvaluator", "SetRoles",
	"SetServiceName", "SetSessionOptions", "SetupMeshSession", "Shutdown",
	"SnapshotHandler", "SubsystemsHandler", "Swarm", "TenantCount",
	"TopologyRouter", "Transport", "TransportManager",
	"UnregisterExternalSession", "UnregisterSession", "UpdateTenantKeys",
	"WebSocketHandler",
}

var goldenRPCServerMethods = []string{
	"ActiveHandlers", "BidiLatencySnapshots", "BidiPhaseSnapshots",
	"BidiTimeoutSnapshots", "DedupStats", "DispatchLatencyTopN",
	"HandleIncomingSessions", "HandleIncomingSessionsForTenant", "Metrics",
	"RecordBidiLatency", "RecordBidiPhaseMarshal", "RecordBidiPhaseSend",
	"RecordBidiPhaseWait", "RecordBidiTimeout", "RegisterRPC",
	"ServeMeshStream", "ServeSession", "SetForwarder", "SetLoadAdvisor",
	"SetNodeInfo",
}

var goldenHandlerRegistryMethods = []string{
	"AllHandlers", "Count", "Dispatch", "DispatchStream", "DispatchTask",
	"GetByRoleMeta", "GetMeta", "GetMiddleware", "Handlers", "IsRoleEnabled",
	"List", "RegisterHandler", "RegisterRPC", "RegisterStream",
	"RegisterTask", "SetEnabledRoles", "SetPlatformTenants", "Use",
	"UseForRole",
}

func assertMethods(t *testing.T, typ reflect.Type, want []string) {
	t.Helper()
	have := make(map[string]bool, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		have[typ.Method(i).Name] = true
	}
	var missing []string
	for _, name := range want {
		if !have[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%s lost %d exported method(s) vs the pre-extraction baseline: %v",
			typ, len(missing), missing)
	}
}

func TestGoldenSurfaceRuntime(t *testing.T) {
	assertMethods(t, reflect.TypeOf((*Runtime)(nil)), goldenRuntimeMethods)
}

func TestGoldenSurfaceRPCServer(t *testing.T) {
	assertMethods(t, reflect.TypeOf((*RPCServer)(nil)), goldenRPCServerMethods)
}

func TestGoldenSurfaceHandlerRegistry(t *testing.T) {
	assertMethods(t, reflect.TypeOf((*handlers.HandlerRegistry)(nil)), goldenHandlerRegistryMethods)
}

// TestGoldenConfigSeams asserts the Phase-0 injection seams exist with the
// exact field names Phase-3 consumers wire (plan §0.3 rows 1/4/5/7).
func TestGoldenConfigSeams(t *testing.T) {
	typ := reflect.TypeOf(Config{})
	for _, field := range []string{
		"Platform", "MeshFallbackStats", "AuthValidator",
		"RegisterDomains", "VerifyBootstrap",
	} {
		if _, ok := typ.FieldByName(field); !ok {
			t.Fatalf("Config lost seam field %q", field)
		}
	}
}
