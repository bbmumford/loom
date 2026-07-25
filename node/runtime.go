/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbmumford/route"
	"github.com/bbmumford/tursoraft/connectors"
	meshdb "github.com/bbmumford/tursoraft/manager"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/anchor"
	"github.com/ORBTR/aether/discovery"
	"github.com/ORBTR/aether/nat"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/reach"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/ledger/storage"
	meshledger "github.com/bbmumford/loom/core/directory/ledger"
	"github.com/bbmumford/loom/core/transport/manager"
	quictransport "github.com/ORBTR/aether/quic"
	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
	"github.com/ORBTR/aether/relay"

	"github.com/gobwas/ws"
	"google.golang.org/grpc"
	wstransport "github.com/ORBTR/aether/websocket"
	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/node/audit"
	"github.com/bbmumford/loom/node/handlers"
	obshealth "github.com/bbmumford/loom/pkg/obshealth"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/ports"
)



// upgradedConn wraps a 101-upgraded HTTP connection for bidirectional use.
// Reads go through resp.Body (which buffers from the TLS conn); writes go
// directly to the captured TLS conn.
type upgradedConn struct {
	reader io.ReadCloser // resp.Body
	writer net.Conn      // captured TLS conn
}

func (c *upgradedConn) Read(b []byte) (int, error)         { return c.reader.Read(b) }
func (c *upgradedConn) Write(b []byte) (int, error)        { return c.writer.Write(b) }
func (c *upgradedConn) Close() error                       { c.reader.Close(); return c.writer.Close() }
func (c *upgradedConn) LocalAddr() net.Addr                { return c.writer.LocalAddr() }
func (c *upgradedConn) RemoteAddr() net.Addr               { return c.writer.RemoteAddr() }
func (c *upgradedConn) SetDeadline(t time.Time) error      { return c.writer.SetDeadline(t) }
func (c *upgradedConn) SetReadDeadline(t time.Time) error  { return c.writer.SetReadDeadline(t) }
func (c *upgradedConn) SetWriteDeadline(t time.Time) error { return c.writer.SetWriteDeadline(t) }

// bootstrapDialResult holds the outcome of a successful bootstrap host dial.
type bootstrapDialResult struct {
	conn     *upgradedConn // bidirectional conn (reads from resp.Body, writes to TLS)
	nodeID   string        // remote node ID from X-VL1-Node-ID header
	resp     *http.Response
	rawConn  net.Conn // underlying TLS connection
}

// dialBootstrapHost dials a bootstrap host via HTTPS + VL1 upgrade and returns
// a bidirectional connection suitable for gossip. Transport-agnostic — produces
// a net.Conn; the caller doesn't care about the underlying TLS implementation.
func (rt *Runtime) dialBootstrapHost(ctx context.Context, host string) (*bootstrapDialResult, error) {
	httpsURL := fmt.Sprintf("https://%s/mesh/vl1", host)
	req, err := http.NewRequestWithContext(ctx, "POST", httpsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// VL1 upgrade headers
	req.Header.Set("Upgrade", "vl1")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("X-VL1-Node-ID", string(rt.identity.NodeID))
	req.Header.Set("X-VL1-UDP-Port", fmt.Sprintf("%d", rt.cfg.VL1.UDPPort))
	if rt.cfg.VL1.FailoverPort > 0 {
		req.Header.Set("X-VL1-Failover-Port", fmt.Sprintf("%d", rt.cfg.VL1.FailoverPort))
	}
	if rt.cfg.ServiceName != "" {
		req.Header.Set("X-VL1-Service-Name", rt.cfg.ServiceName)
	}
	req.Header.Set("X-VL1-Region", rt.cfg.Platform.Region())
	if len(rt.cfg.Roles) > 0 {
		req.Header.Set("X-VL1-Roles", strings.Join(rt.cfg.Roles, ","))
	}
	if privateIP := rt.discoverPrivateIP(); privateIP != "" {
		req.Header.Set("X-VL1-Private-IP", privateIP)
	}
	if ip := rt.getPublicIP(); ip != "" {
		req.Header.Set("X-VL1-Public-IP", ip)
	}

	// Capture the TLS conn for bidirectional use after 101 upgrade
	var capturedConn net.Conn
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 5 * time.Second}
			rawConn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tcpConn, ok := rawConn.(*net.TCPConn); ok {
				_ = tcpConn.SetKeepAlive(true)
				_ = tcpConn.SetKeepAlivePeriod(5 * time.Second)
			}
			tlsHost := addr
			if h, _, err := net.SplitHostPort(addr); err == nil {
				tlsHost = h
			}
			tlsConn := tls.Client(rawConn, &tls.Config{ServerName: tlsHost})
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, err
			}
			capturedConn = tlsConn
			return tlsConn, nil
		},
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		rt.recordVL1DialFailure()
		return nil, fmt.Errorf("dial %s: %w", host, err)
	}

	if resp.StatusCode != http.StatusSwitchingProtocols {
		resp.Body.Close()
		rt.recordVL1DialFailure()
		return nil, fmt.Errorf("%s rejected VL1 upgrade (status %d)", host, resp.StatusCode)
	}

	remoteNodeID := aether.NormalizeNodeID(resp.Header.Get("X-VL1-Node-ID"))
	if remoteNodeID == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("%s did not provide node ID", host)
	}

	// Correct public IP if bootstrap observed a different one
	observedIP := resp.Header.Get("X-VL1-Observed-IP")
	if cur := rt.getPublicIP(); observedIP != "" && observedIP != "0.0.0.0" && observedIP != "127.0.0.1" && observedIP != cur {
		log.Printf("[NODE] Bootstrap: public IP corrected to %s (observed by %s, was %s)", observedIP, host, cur)
		rt.setPublicIP(observedIP)
		// Re-publish so peers see the corrected address within one
		// gossip round instead of waiting for the next periodic
		// PeerRecord refresh. republishWithPublicIP is a no-op when
		// the swarm publisher hasn't been wired yet (early-boot path).
		rt.republishWithPublicIP()
	}

	// Phase 1 (LAD-unified): fetch the anchor's signed ReachRecord and append
	// to our local ledger via the same TopicReach path that gossip uses.
	// Replaces publishBootstrapMember (which wrote a member-only record from
	// HTTP headers with NO addresses, leaving anchors invisible to
	// bestAddress and forcing anchor-adjacent traffic onto WebSocket). The
	// fetch is best-effort — if it fails, gossip propagation will eventually
	// deliver the reach record. We do NOT fall back to the header-derived
	// path, restoring the "one signed envelope" invariant.
	rt.ingestPeerReachRecord(ctx, host, remoteNodeID, resp)

	// Mark both nodes alive in gossip liveness
	if rt.cache != nil {
		rt.cache.RecordGossipSeen(remoteNodeID)
		rt.cache.RecordGossipSeen(string(rt.identity.NodeID))
	}

	conn := &upgradedConn{reader: resp.Body, writer: capturedConn}
	return &bootstrapDialResult{
		conn:    conn,
		nodeID:  remoteNodeID,
		resp:    resp,
		rawConn: capturedConn,
	}, nil
}

// ingestPeerReachRecord fetches a peer's signed ReachRecord from the
// /mesh/reach/{nodeID} endpoint on the bootstrap host and appends it to
// the local ledger via the standard TopicReach path. This is the
// LAD-unified replacement for publishBootstrapMember: identity comes
// from the signed reach metadata (synthesized into a MemberRecord view
// on read), addresses come from the reach addresses themselves — one
// signed envelope, no parallel publish paths.
//
// Best-effort: on any failure (HTTP error, signature verification fail,
// ledger append error) we log and proceed. Gossip propagation will
// eventually deliver the record; the request-fetch is a startup
// shortcut so anchors don't sit unresolved through the first gossip
// round (~10s window). We deliberately do NOT fall back to the
// header-derived member-only path — that path is removed.
func (rt *Runtime) ingestPeerReachRecord(ctx context.Context, host, remoteNodeID string, bootResp *http.Response) {
	if remoteNodeID == "" || host == "" {
		return
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://%s/mesh/reach/%s", host, remoteNodeID)
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, url, nil)
	if err != nil {
		log.Printf("[NODE] Bootstrap: build reach-fetch request: %v", err)
		return
	}

	// Reuse the bootstrap TLS config — same host, same certificate validation.
	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			Proxy:             http.ProxyFromEnvironment,
			ForceAttemptHTTP2: false,
			TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[NODE] Bootstrap: fetch %s/mesh/reach/%s: %v (gossip will recover)", host, remoteNodeID[:12], err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[NODE] Bootstrap: %s/mesh/reach/%s status %d (gossip will recover)", host, remoteNodeID[:12], resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		log.Printf("[NODE] Bootstrap: read reach body: %v", err)
		return
	}
	if len(body) == 0 {
		return
	}

	// Verify the signature before trusting the record. The reach package
	// validates the signature against the embedded public key and ensures
	// the NodeID matches the signer.
	var rec reach.ReachRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		log.Printf("[NODE] Bootstrap: unmarshal reach record from %s: %v", host, err)
		return
	}
	if err := reach.Verify(rec); err != nil {
		log.Printf("[NODE] Bootstrap: reach signature verify failed for %s: %v", remoteNodeID[:12], err)
		return
	}
	if rec.NodeID != remoteNodeID {
		log.Printf("[NODE] Bootstrap: reach NodeID mismatch (rec=%s expected=%s) — possible MITM, dropping", rec.NodeID[:12], remoteNodeID[:12])
		return
	}

	// Append to the ledger via the standard TopicReach path. The ledger
	// cache derives MemberRecord views on read from ReachRecord.Metadata,
	// so the member view is populated automatically.
	if err := rt.ledger.Append(ctx, lad.Record{
		Topic:        lad.TopicReach,
		NodeID:       remoteNodeID,
		Body:         json.RawMessage(body),
		Timestamp:    time.Now().UTC(),
		LamportClock: rt.lamportClock.Tick(),
	}); err != nil {
		log.Printf("[NODE] Bootstrap: ledger.Append reach for %s: %v", remoteNodeID[:12], err)
		return
	}
	log.Printf("[NODE] Bootstrap: ingested signed reach for %s from %s (addrs=%d)", remoteNodeID[:12], host, len(rec.Addresses))
}

type rateLimitEntry struct {
	tokens     float64
	lastUpdate time.Time
}

// Runtime manages the lifecycle of a mesh node
type Runtime struct {
	cfg Config

	// sessionOpts are the Aether SessionOptions applied to every new
	// adapter session this runtime creates. Defaults to
	// aether.DefaultSessionOptions() — overridable via SetSessionOptions
	// before the first session is established so operator tuning
	// (FrameLogging, MaxConcurrentStreams, SessionIdleTimeout, etc.)
	// actually reaches production.
	sessionOpts aether.SessionOptions

	// natBehaviour is the RFC 5780 classification of our own NAT —
	// populated at startup by discoverPublicIP via DetectNATBehaviour.
	// Consumers ( peer_connections / relay fallback) read this to
	// pick the optimal traversal strategy: peers behind endpoint-
	// independent mappings can direct-dial, symmetric-mapped peers
	// need coordinated hole-punching, and address-port-dependent
	// filters benefit from birthday-port prediction.
	natBehaviourMu sync.RWMutex
	natBehaviour   nat.NATBehaviour
	natStrategy    *nat.Strategy

	// punchCoord builds, signs and verifies coordinated hole-punch
	// payloads (Phase 2, fleet NAT traversal). It owns no transport —
	// the responder side is exposed over the mesh RPC path via
	// holepunchRPCHandler, the initiator side drives it from connectPeer.
	punchCoord *punchCoordinator

	// Core components
	identity         *NodeIdentity
	transportMgr     *manager.TransportManager // per-tenant transport routing
	sharedTransport  *manager.SharedTransport  // shared preamble-based transport (may be nil)
	listenTransports []aether.Transport     // unique transports needing Listen()
	listenTenantIDs  []string                  // parallel: tenant ID per listener (""=from session)
	ledger           lad.Ledger
	cache            *ladcache.DirectoryCache
	publicIPMu       sync.RWMutex // MESH-A01: guards publicIP (written by STUN + bootstrap goroutines, read by HTTP handlers + punch)
	publicIP         string       // discovered via STUN after listener starts; access via getPublicIP/setPublicIP
	connMgr             *ConnectionManager // multi-transport peer connection manager (allocated synchronously in Initialize before listener)
	connMgrPostInitDone bool               // guards the post-init wiring goroutine in bootstrapPeers from re-running on re-bootstrap

	// satMu guards the saturation hysteresis latch. satActive is the current
	// latched state: it flips on at saturationHighWater utilization and off at
	// saturationLowWater, so a budget hovering at the boundary does not flap
	// the advertised "saturated" tag on every republish.
	satMu     sync.Mutex
	satActive bool
	peerStore        *FilePeerStore         // persisted peer list for cold-restart warm dials
	discoverers      []discovery.Discoverer // optional discovery layers (DNS SRV, mDNS, etc.)
	// (dormancy managed inline by ConnectionManager.transports)
	syncer           *gossip.Synchronizer
	meshDB           *meshdb.Manager

	// Optional components
	rpcServer            *RPCServer
	rpcForwarder         *runtimeForwarder
	healthCheck          *HealthCheck
	sessionHealthMonitor *SessionHealthMonitor
	circuitBreaker       *CircuitBreaker
	auditLogger          *audit.AuditLogger
	tlsConfig             *TLSConfig
	healthEvaluator       HealthEvaluator      // unified mesh+HTTP health evaluator
	ladSnapshot           *LADSnapshotCache    // refreshing snapshot of LAD directory state — readers never block on directory I/O
	selfHealth            *SelfHealthMonitor   // observability self-health monitor (watches for staleness)
	healthRegistry        *obshealth.Registry  // platform-wide degradedSubsystem sink — mesh/node/monitoring/billing/etc register here
	lamportClock         LamportClock       // causal ordering for LAD records
	partitionDetector    *PartitionDetector // split-brain detection via clock divergence across peers

	// Swarm integration — unified PeerRecord publisher + RoleTable +
	// AddressTable. Replaces the legacy reach + role publish paths.
	// Initialized lazily via InitSwarm(); nil until the consumer wires
	// it (typically inside Runtime.Initialize after identity load +
	// rpc.Registry build).
	swarm *SwarmIntegration

	// swarmGradeHookInstalled latches the AddEventSubscriber call that
	// republishes our PeerRecord whenever the local BestActiveGrade
	// changes. PublishRPCHandlersToLAD is invoked on every role/handler
	// change and on every endpoint cold start — without the latch,
	// every call would append another subscriber and grade-upgrade
	// would emit log + publish N times per event.
	swarmGradeHookMu        sync.Mutex
	swarmGradeHookInstalled bool

	// Route integration — multi-hop path advertisement and best-path
	// selection. Wired on top of the swarm fabric via InitRoute(); the
	// dial path consults route.Router.BestPath when no direct candidate
	// exists. Stored via atomic.Pointer so the SessionHook closure (which
	// fires from arbitrary transport goroutines) can read it without a
	// lock while InitRoute publishes the integration. Load() returns nil
	// until the consumer calls InitRoute.
	route atomic.Pointer[RouteIntegration]

	// Anchor service
	anchorGenerator *anchor.Generator
	// anchorLeader removed — all anchor-capable nodes generate snapshots independently
	anchorCancel    context.CancelFunc
	latestSnapshot  *anchor.Snapshot
	snapshotMu      sync.RWMutex

	// VL1/LAD metrics
	vl1Metrics struct {
		sync.RWMutex
		activeSessions     int
		totalConnections   uint64
		failedDials        uint64
		lastSyncTime       time.Time
		syncLag            time.Duration
		ledgerAppendErrors uint64
	}

	// Rate limiting for snapshots
	snapshotRateLimitMu sync.Mutex
	snapshotRateLimit   map[string]*rateLimitEntry

	// Lifecycle management
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	wg           sync.WaitGroup

	// Phase-1 runtime role activation (role_activation.go) + takeover
	// engine (role_takeover.go). roleActivation is always non-nil after
	// Initialize; rpcRegistry is captured by InitSwarm so activation can
	// re-publish handler advertisements.
	roleActivation *roleActivationManager
	rpcRegistry    *rpc.Registry
	takeover       *TakeoverEngine
	composeReg     *compose.Registry

	// bootstrap connections accepted when running as a bootstrap host
	bootstrapConns   map[string]net.Conn
	bootstrapConnsMu sync.Mutex

	// Active outbound sessions by nodeID — for RPC dispatch reuse.
	// Populated by bootstrap gossip connections so RPC can reuse them
	// instead of dialing new connections (which may fail cross-origin).
	activeConns   map[string]net.Conn
	activeConnsMu sync.RWMutex

	// WebSocket gossip connections accepted via /mesh/ws
	wsConns   map[string]net.Conn
	wsConnsMu sync.Mutex

}

// Initialize creates and starts a new mesh node runtime
func Initialize(cfg Config) (*Runtime, error) {
	// Nil-safety: fall back to the generic dev platform. Cloud deployments
	// must inject their real platform implementation (ports/seams.go) —
	// this default has no Fly/AWS/GCP/k8s bind or origin semantics.
	if cfg.Platform == nil {
		cfg.Platform = ports.DevPlatform()
		log.Printf("[NODE] Config.Platform not injected — using generic dev platform (no cloud bind/origin semantics)")
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Apply config defaults BEFORE creating Runtime (cfg is copied by value)
	if cfg.GossipInterval <= 0 {
		cfg.GossipInterval = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	rt := &Runtime{
		cfg:         cfg,
		ctx:         ctx,
		cancel:      cancel,
		sessionOpts: aether.DefaultSessionOptions(),
		// healthRegistry is initialized eagerly with the platform-wide
		// allow-list so any subsystem that starts up during Initialize
		// (HealthCheck, SelfHealthMonitor, billing webhooks bound
		// via SetWebhookDeps, uptime runner, etc.) can Mark/Clear from
		// the moment it comes online. Fail-closed on unknown IDs so
		// per-event compound keys cannot leak into the map.
		healthRegistry: obshealth.New(obshealth.AllowedSubsystems()),
	}

	// Initialize the partition detector with the runtime's Lamport clock
	// so it can compare local progress against peer-reported clocks.
	rt.partitionDetector = NewPartitionDetector(&rt.lamportClock)

	// Ensure ServiceName is always set — required for topology, member records, and peer discovery
	if cfg.ServiceName == "" {
		hostname, _ := os.Hostname()
		if hostname != "" {
			cfg.ServiceName = hostname
		} else {
			cfg.ServiceName = "unknown"
		}
		log.Printf("[NODE] Warning: ServiceName not configured, defaulting to %q", cfg.ServiceName)
	}

	// Step 1: Load or generate node identity
	log.Printf("[NODE] Initializing mesh node runtime (service: %s, gossip: %s)", cfg.ServiceName, cfg.GossipInterval)

	// rev-073: identity is now random-generated on first run and persisted
	// under cfg.DataDir, replacing the hostname-derived scheme (which
	// made the private key equivalent to a publicly-documented value).
	identity, err := GenerateIdentity(ctx, cfg.DataDir)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("generate identity: %w", err)
	}
	rt.identity = identity

	// Step 2: Initialize audit logger
	if cfg.Audit.Enabled {
		auditLogger, err := audit.NewAuditLogger(cfg.Audit.LogPath, true)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create audit logger: %w", err)
		}
		rt.auditLogger = auditLogger
		log.Printf("[NODE] Audit logging enabled (path: %s)", cfg.Audit.LogPath)
	}

	// Step 4: Initialize circuit breaker
	if cfg.CircuitBreaker.Enabled {
		rt.circuitBreaker = NewCircuitBreaker(cfg.CircuitBreaker.Threshold, cfg.CircuitBreaker.ResetTime)
		log.Printf("[NODE] Circuit breaker enabled (threshold: %d, reset: %v)",
			cfg.CircuitBreaker.Threshold, cfg.CircuitBreaker.ResetTime)
	}

	// Step 4b: Initialize session health monitor with default config
	rt.sessionHealthMonitor = NewSessionHealthMonitor(relay.DefaultHealthConfig())
	rt.sessionHealthMonitor.SetEvictCallback(func(nodeID aether.NodeID, reason string) {
		log.Printf("[NODE] Session evicted for %s: %s", nodeID, reason)
		rt.recordVL1Disconnection()
	})

	// Step 5: Initialize RPC server with HandlerRegistry
	registry := handlers.NewHandlerRegistry()
	registry.SetEnabledRoles([]string{"system"})

	rt.rpcServer = NewRPCServer(registry)
	rt.rpcServer.SetAuthValidator(cfg.AuthValidator)
	rt.roleActivation = newRoleActivationManager(rt)
	rt.rpcServer.SetLoadAdvisor(rt)
	rt.rpcServer.SetNodeInfo(string(rt.identity.NodeID), cfg.Platform.Region(), cfg.ServiceName)

	// Install the local-dispatch short-circuit: rpc.Call / rpc.CallRaw
	// now consult this registry FIRST and skip the mesh when the handler
	// is hosted here. Avoids round-tripping to ourselves over a stream.
	// See mesh/node/rpc_local_dispatch.go for the adapter.
	installLocalDispatcher(registry)

	// Enable mesh-routed RPC forwarding — unknown handlers are forwarded to
	// peer nodes instead of failing with "unknown handler".
	rt.wireForwarder()

	// Register built-in handlers on the registry
	_ = registry.RegisterRPC(&PingRPCHandler{})
	_ = registry.RegisterRPC(&StatusRPCHandler{identity: identity, startTime: rt.rpcServer.startTime})

	// Phase 2 NAT traversal: the punch coordinator and its responder-side
	// RPC handler. NATBehaviour / localPunchCandidates are snapshot
	// functions read fresh on each build, so registering here — before
	// STUN classification and transport listen — is safe: the payload
	// always reflects current state when a peer actually invokes it.
	rt.punchCoord = newPunchCoordinator(
		rt.identity.NodeID,
		rt.identity.PrivateKey,
		rt.NATBehaviour,
		rt.localPunchCandidates,
	)
	_ = registry.RegisterRPC(&holepunchRPCHandler{
		coord: rt.punchCoord,
		probe: rt.punchProbe,
	})

	// Step 6: Initialize VL1/LAD if enabled
	ladEnabled := cfg.LAD.Storage != ""
	if cfg.VL1.Enabled || ladEnabled {
		log.Printf("[NODE] Initializing VL1/LAD overlay aether...")

		// Initialize LAD directory cache (always needed for VL1)
		rt.cache = ladcache.NewDirectoryCache()
		// Identify which NodeID this cache is "self" for so eviction
		// never creates a self-referential tombstone that would block
		// the node's own future re-announcements. Root cause of the
		// observed 94-tombstones-vs-2-live-members convergence stall
		// on help.orbtr.io after fleet-wide deploys.
		rt.cache.SetLocalNodeID(string(rt.identity.NodeID))
		// Re-sign envelopes rebuilt from typed projections inside
		// cache.Dump/DumpSince so the gossip emit path produces wire
		// records the receiver's signedTopicACL can verify.
		rt.cache.SetDumpSigningKey(rt.identity.PrivateKey)
		rt.cache.StartEviction(ladcache.DefaultEvictionInterval)

		// Register HSTLES topic configurations with the Ledger cache.
		// All HSTLES topics use OverwriteMerge (latest timestamp wins) and NodeIDKey.
		rt.cache.RegisterTopic(lad.TopicMember, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey})
		rt.cache.RegisterTopic(lad.TopicRole, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey})
		rt.cache.RegisterTopic(lad.TopicReach, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey, ExpiryEnabled: true})
		rt.cache.RegisterTopic(lad.TopicLatency, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey, ExpiryEnabled: true})
		rt.cache.RegisterTopic(lad.TopicKeyOps, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey})
		rt.cache.RegisterTopic(lad.TopicQuorum, lad.TopicConfig{Merge: lad.OverwriteMerge, Key: lad.NodeIDKey})

		// Signed-topic enforcement: every identity-mutating topic must
		// carry a valid ed25519 signature over the full LADRecord
		// content (ledger v0.0.11 signatureContent covers every wire
		// field) signed by the AuthorPubKey on the record. Without
		// these ACL hooks any gossip peer could forge a member /
		// role / reach / key-ops record by minting one without a
		// signature — the wire fields would be accepted by Apply()
		// regardless. RegisterACL gates Apply on VerifyRecord so
		// unsigned + invalid-signature records are dropped at the
		// cache boundary, before they touch persistent state or
		// downstream consumers (RoleTable / AddressTable / ReachCache).
		signedTopicACL := func(topic string, authorPubKey []byte, rec lad.Record) error {
			if !lad.VerifyRecord(rec) {
				return lad.ErrSignatureInvalid
			}
			return nil
		}
		rt.cache.RegisterACL(lad.TopicMember, signedTopicACL)
		rt.cache.RegisterACL(lad.TopicRole, signedTopicACL)
		rt.cache.RegisterACL(lad.TopicReach, signedTopicACL)
		rt.cache.RegisterACL(lad.TopicKeyOps, signedTopicACL)
		rt.cache.RegisterACL(lad.TopicQuorum, signedTopicACL)
		// TopicLatency intentionally NOT signed — it's frequently
		// updated, low-trust telemetry that any honest mesh peer can
		// publish without an identity proof.

		log.Printf("[NODE] LAD directory cache initialized (6 topics registered, TTL eviction enabled, signed-topic ACL on member/role/reach/keyops/quorum)")

		// Initialize the LAD snapshot cache that observability handlers
		// (HealthEvaluator, /mesh-debug, /mesh-topology, etc.) will read
		// from. A single background goroutine pulls fresh state every
		// RefreshInterval (10s) with PerCallTimeout (3s) bounding each
		// directory method. Readers swap-in snapshots via atomic.Pointer
		// — request handlers never touch the directory directly.
		rt.ladSnapshot = NewLADSnapshotCache(rt.cache, DefaultLADSnapshotCacheConfig())
		rt.ladSnapshot.Start()
		log.Printf("[NODE] LAD snapshot cache started (refresh=10s, per-call-timeout=3s)")

		// Initialize ledger based on LAD.Storage config
		switch cfg.LAD.Storage {
		case "local":
			// Create pure local database for LAD (mesh-ephemeral state)
			// No Turso sync - ephemeral peer discovery records only
			ladDBPath := filepath.Join(cfg.LAD.LocalDir, "lad.db")
			if err := os.MkdirAll(cfg.LAD.LocalDir, 0o755); err != nil {
				cancel()
				return nil, fmt.Errorf("create LAD directory: %w", err)
			}

			ladHandle, err := connectors.MakeConnector(ladDBPath, "", "", "") // pure local, no encryption
			if err != nil {
				cancel()
				return nil, fmt.Errorf("create LAD database connector: %w", err)
			}
			ladDB := ladHandle.Database() // Extract *sql.DB

			// Configure ledger with retention and compaction
			ledgerCfg := storage.LedgerConfig{
				RetentionDays:      30,             // Keep 30 days of history
				CompactionInterval: 24 * time.Hour, // Compact daily
			}

			meshLedger, err := storage.NewLocalDBLedger(ladDB, ledgerCfg)
			if err != nil {
				cancel()
				return nil, fmt.Errorf("create LAD ledger: %w", err)
			}
			rt.ledger = newSigningLedger(meshLedger, rt.identity.PrivateKey)
			log.Printf("[NODE] LAD ledger initialized (storage=local, 30-day retention, signing-wrapper enabled)")

		case "memory":
			// In-memory ledger for ephemeral/testing deployments
			rt.ledger = newSigningLedger(storage.NewMemoryLedger(storage.DefaultMemoryLedgerConfig()), rt.identity.PrivateKey)
			log.Printf("[NODE] LAD ledger initialized (storage=memory, 1hr retention, 10k records/topic, signing-wrapper enabled)")

		case "mesh":
			// Mesh-replicated LAD via Tursoraft for multi-node deployments
			if cfg.LAD.RaftBindAddr == "" {
				cancel()
				return nil, fmt.Errorf("LAD mesh storage requires RaftBindAddr to be configured")
			}
			if cfg.LAD.LocalDir == "" {
				cancel()
				return nil, fmt.Errorf("LAD mesh storage requires LocalDir to be configured")
			}

			// Use configured values with sensible defaults
			retentionDays := cfg.LAD.RetentionDays
			if retentionDays <= 0 {
				retentionDays = 30
			}

			compactionInterval := cfg.LAD.CompactionInterval
			if compactionInterval <= 0 {
				compactionInterval = 24 * time.Hour
			}

			enableBloom := true
			if cfg.LAD.EnableBloomFilters != nil {
				enableBloom = *cfg.LAD.EnableBloomFilters
			}

			// Parse bootstrap hosts into peer list for mesh LAD
			var peers []string
			if cfg.LAD.BootstrapHosts != "" {
				for _, p := range strings.Split(cfg.LAD.BootstrapHosts, ",") {
					if p = strings.TrimSpace(p); p != "" {
						peers = append(peers, p)
					}
				}
			}

			// Configure tursoraft for LAD-only mesh replication (pure local, no Turso cloud)
			meshCfg := meshledger.MeshLedgerConfig{
				GroupName:          "mesh",
				DBName:             "lad",
				Peers:              peers, // Pass peers directly (envreq fallback in MeshLedger)
				RetentionDays:      retentionDays,
				CompactionInterval: compactionInterval,
				EnableBloomFilters: enableBloom,
				ManagerConfig: meshdb.ManagerConfig{
					UseEmbedded: true, // Pure local embedded libSQL
					Groups: []meshdb.GroupConfig{{
						GroupName: "mesh",
						DBNames:   []string{"lad"},
						PureLocal: true, // No Turso sync - local only
					}},
					RaftConfig: meshdb.RaftConfig{
						RaftBindAddr: cfg.LAD.RaftBindAddr,
						LocalDir:     filepath.Join(cfg.LAD.LocalDir, "raft"),
						SnapshotDir:  filepath.Join(cfg.LAD.LocalDir, "snapshots"),
					},
					SyncInterval: 0, // No Turso sync for LAD
					SnapMinGap:   time.Minute,
				},
			}

			meshLedger, err := meshledger.NewMeshLedger(ctx, meshCfg)
			if err != nil {
				cancel()
				return nil, fmt.Errorf("create mesh LAD ledger: %w", err)
			}
			rt.ledger = newSigningLedger(meshLedger, rt.identity.PrivateKey)
			log.Printf("[NODE] LAD ledger initialized (storage=mesh, raft=%s, retention=%dd, peers=%s, signing-wrapper enabled)",
				cfg.LAD.RaftBindAddr, retentionDays, cfg.LAD.BootstrapHosts)

		default:
			// LAD disabled but VL1 enabled - use minimal memory ledger
			rt.ledger = newSigningLedger(storage.NewMemoryLedger(storage.DefaultMemoryLedgerConfig()), rt.identity.PrivateKey)
			log.Printf("[NODE] LAD ledger initialized (VL1 only, memory-backed, signing-wrapper enabled)")
		}

		// Initialize synchronizer to keep cache in sync with ledger
		rt.syncer = gossip.NewSynchronizer(rt.ledger, rt.cache)
		log.Printf("[NODE] LAD synchronizer initialized")

		if cfg.VL1.Enabled {
			// Create TransportManager for tenant-aware transport routing
			rt.transportMgr = manager.New()

			// Base Noise config shared by all transports
			baseCfg := noise.NoiseTransportConfig{
				LocalNode:        rt.identity.NodeID,
				PrivateKey:       rt.identity.PrivateKey,
				HandshakeTimeout: 3 * time.Second,
				MaxPacketSize:    64 * 1024,
				STUNConfig:       aether.DefaultSTUNConfig(),
				BindResolver:     cfg.Platform,
				RelayConfig: relay.RelayConfig{
					Enabled:   cfg.VL1.Relay.Enabled,
					MaxRate:   cfg.VL1.Relay.MaxRate,
					TicketTTL: cfg.VL1.Relay.TicketTTL,
				},
			}

			if len(cfg.Tenants) == 0 {
				// ── Single-tenant fallback (backward compat) ──────────────────
				// No Tenants configured — create a single NoiseTransport from
				// cfg.NetworkKeys and register it as "default".
				singleCfg := baseCfg
				singleCfg.NetworkKeys = cfg.NetworkKeys
				singleCfg.ListenAddr = fmt.Sprintf(":%d", cfg.VL1.UDPPort)

				tr, err := noise.NewNoiseTransport(singleCfg)
				if err != nil {
					cancel()
					return nil, fmt.Errorf("create VL1 transport: %w", err)
				}
				rt.attachIntraOrgForwarder(tr, "default")
				rt.transportMgr.Register(aether.ScopeID("default"), tr)
				rt.listenTransports = append(rt.listenTransports, tr)
				rt.listenTenantIDs = append(rt.listenTenantIDs, "default")

				// Register with global transport registry for selector fallback
				aether.RegisterTransport(aether.ProtocolVL1, tr)
				log.Printf("[NODE] VL1 Noise transport initialized (single-tenant, STUN: %v, Relay: %v)",
					singleCfg.STUNConfig.Enabled, singleCfg.RelayConfig.Enabled)
			} else {
				// ── Multi-tenant mode ────────────────────────────────────────
				// Determine if we need a shared transport (any non-dedicated tenant).
				hasShared := false
				for _, tc := range cfg.Tenants {
					if !tc.Dedicated {
						hasShared = true
						break
					}
				}

				// Create shared transport on the main VL1 UDP port if needed.
				if hasShared {
					sharedCfg := baseCfg
					sharedCfg.NetworkKeys = cfg.NetworkKeys // fallback keys for STUN/default
					sharedCfg.ListenAddr = fmt.Sprintf(":%d", cfg.VL1.UDPPort)

					st, err := manager.NewSharedTransport(sharedCfg)
					if err != nil {
						cancel()
						return nil, fmt.Errorf("create shared VL1 transport: %w", err)
					}
					rt.attachIntraOrgForwarder(st.Inner(), "shared")
					rt.sharedTransport = st
					rt.listenTransports = append(rt.listenTransports, st)
					rt.listenTenantIDs = append(rt.listenTenantIDs, "") // resolved from session preamble
					log.Printf("[NODE] VL1 shared transport initialized on :%d", cfg.VL1.UDPPort)
				}

				// Register each tenant.
				for _, tc := range cfg.Tenants {
					tid := aether.ScopeID(tc.TenantID)

					if tc.Dedicated {
						// Dedicated: own NoiseTransport on its own port.
						dedCfg := baseCfg
						dedCfg.NetworkKeys = tc.NetworkKeys
						dedCfg.ListenAddr = fmt.Sprintf(":%d", tc.UDPPort)

						tr, err := noise.NewNoiseTransport(dedCfg)
						if err != nil {
							cancel()
							return nil, fmt.Errorf("create dedicated transport for tenant %s: %w", tc.TenantID, err)
						}
						rt.attachIntraOrgForwarder(tr, "tenant:"+tc.TenantID)
						rt.transportMgr.Register(tid, tr)
						rt.listenTransports = append(rt.listenTransports, tr)
						rt.listenTenantIDs = append(rt.listenTenantIDs, tc.TenantID)
						log.Printf("[NODE] VL1 dedicated transport for tenant %s on :%d", tc.TenantID, tc.UDPPort)
					} else {
						// Shared: register tenant keys with the shared transport,
						// then map tenant → shared transport in the manager.
						keys := make([][]byte, len(tc.NetworkKeys))
						for i, k := range tc.NetworkKeys {
							keys[i] = []byte(k)
						}
						if err := rt.sharedTransport.RegisterTenant(tid, keys); err != nil {
							cancel()
							return nil, fmt.Errorf("register tenant %s on shared transport: %w", tc.TenantID, err)
						}
						rt.transportMgr.Register(tid, rt.sharedTransport)
						log.Printf("[NODE] VL1 tenant %s registered on shared transport", tc.TenantID)
					}
				}

				// Register first available transport with global registry for selector fallback.
				if len(rt.listenTransports) > 0 {
					aether.RegisterTransport(aether.ProtocolVL1, rt.listenTransports[0])
				}
				log.Printf("[NODE] VL1 multi-tenant initialized (%d tenants, %d transports)",
					len(cfg.Tenants), len(rt.listenTransports))
			}

			// Start session health monitor for VL1 sessions
			rt.sessionHealthMonitor.Start()
			log.Printf("[NODE] Session health monitor started")

			// SYNCHRONOUSLY allocate ConnectionManager BEFORE bootstrap
			// or listener startup. Listener accepts (started below at
			// startVL1Listener) call AcceptIncomingMeshSession, which
			// guards on `if rt.connMgr != nil`. Previously connMgr was
			// allocated inside a goroutine spawned during bootstrapPeers,
			// racing the listener — sessions arriving before the goroutine
			// completed silently became orphans (SetupMeshSession had
			// already created the NoiseSession + started its goroutines;
			// gossip flowed; but the dispatch registry never saw them).
			//
			// Allocating here guarantees rt.connMgr is non-nil from the
			// moment any session-accept path can fire. The QUIC packet
			// conn from the noise listener isn't available yet — QUIC
			// gracefully falls back to its own socket (see
			// peer_connections.go:426). StartMeshServices spawns its own
			// background goroutines (StreamGC, AdaptiveController) so
			// it's safe to call synchronously.
			//
			// connMgr.Start (the long-running ticker loop) still runs in
			// the goroutine spawned by bootstrapPeers — that's the only
			// async work remaining.
			log.Printf("[NODE] Initializing ConnectionManager...")
			rt.connMgr = NewConnectionManager(rt)
			log.Printf("[NODE] ConnectionManager initialized")

			// Wire the swarm fabric + route engine here as Runtime
			// invariants — not deferred to PublishRPCHandlersToLAD.
			// Endpoints that never call PublishRPCHandlersToLAD
			// (api.hstles, login.hstles, monitoring.hstles,
			// failover.hstles, support.hstles, orbtr.io, …) previously
			// left rt.swarm and rt.route nil and emitted zero route
			// advertisements; every dispatch from those nodes had to
			// fall back to LAD-latency lookups. Wiring here guarantees
			// the route engine is live the moment any session can be
			// accepted.
			//
			// InitSwarm is constructed with a nil rpc.Registry; the
			// endpoint's later PublishRPCHandlersToLAD call threads
			// its registry through PeerPublisher.SetRegistry (InitSwarm
			// itself is idempotent and returns the existing integration).
			//
			// Transit mode is derived from the configured roles — anchor
			// nodes forward multi-hop traffic for any tenant peer; other
			// nodes start TransitNone and can opt in later via
			// SetTransitMode. cfg.Roles already includes "anchor" for
			// anchor-capable nodes because Step 8 (anchor service) below
			// appends it; rt.isAnchorCapable() covers the case where
			// roles aren't statically configured.
			if _, err := rt.InitSwarm(nil); err != nil {
				log.Printf("[NODE] InitSwarm failed at Initialize: %v", err)
			} else {
				transit := route.TransitNone
				if rt.isAnchorCapable() {
					transit = route.TransitFull
				} else {
					for _, r := range rt.cfg.Roles {
						if r == "anchor" {
							transit = route.TransitFull
							break
						}
					}
				}
				if _, err := rt.InitRoute(rt.ctx, transit); err != nil {
					log.Printf("[NODE] InitRoute failed at Initialize: %v", err)
				} else {
					log.Printf("[NODE] Route engine wired (transit=%v)", transit)
				}
			}

			// MESH-A06: start the scaler / AdaptiveController reader goroutines
			// only AFTER rt.swarm is published by InitSwarm above. Those readers
			// dereference m.rt.swarm.AddressTable/RoleTable; launching them before
			// the swarm-pointer assignment raced the write (benign to the nil-
			// guarded readers, but a real data race). Go's memory model makes the
			// assignment visible to any goroutine started after it.
			StartMeshServices(rt.ctx, rt.connMgr)
			log.Printf("[NODE] Aether mesh services started")

			// MESH-G02: poll the split-brain detector so clock divergence /
			// silent-peer conditions are actually reported (Detect/LogStatus
			// previously had no runtime caller). Detect also evicts long-departed
			// peers, so this doubles as the detector's janitor.
			if rt.partitionDetector != nil {
				rt.GoCtx("partition-detector", func(ctx context.Context) {
					t := time.NewTicker(2 * time.Minute)
					defer t.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-t.C:
							rt.partitionDetector.LogStatus()
						}
					}
				})
			}

			// Create directory adapter that wraps our cache to match VL1 interface
			// directoryAdapter := &directoryAdapter{cache: rt.cache}

			// Create VL1 manager with custom timing from config
			/*
				managerCfg := aether.TransportConfig{
					DialTimeout:       cfg.VL1.DialTimeout,
					HandshakeTimeout:  3 * time.Second,
					KeepAliveInterval: cfg.VL1.KeepAliveInterval,
					SuspectTimeout:    10 * time.Second,
					DeadTimeout:       20 * time.Second,
					MaxPathCount:      cfg.VL1.MaxPeers,
				}
			*/

			// Detect region for topology-aware routing
			region := rt.cfg.Platform.Region()
			log.Printf("[NODE] Host region detected: %s", region)

			/*
				rt.vl1Manager = aether.NewManager(
					transport,
					directoryAdapter, // Use adapter to bridge DirectoryCache to VL1 Directory interface
					aether.WithTransportConfig(managerCfg),
					aether.WithPathSelector(aether.NewPathSelector(aether.NewDefaultPathScorer(region))),
				)
				log.Printf("[NODE] VL1 manager initialized (UDP port: %d, max peers: %d)",
					cfg.VL1.UDPPort, cfg.VL1.MaxPeers)
			*/

			// Bootstrap: VL1 connections first (establish actual sessions),
			// then snapshot (populate directory with additional peers).
			if err := rt.bootstrapPeers(ctx, cfg); err != nil {
				log.Printf("[NODE] Warning: bootstrap peer discovery failed: %v", err)
				// Non-fatal - node can still function, re-bootstrap will retry
			}

			// Fetch directory snapshot from anchor nodes to discover additional peers
			if _, err := rt.bootstrapFromAnchor(ctx, cfg); err != nil {
				log.Printf("[NODE] Warning: anchor snapshot fetch failed: %v", err)
			}

			// Start UDP listener if port is configured
			if cfg.VL1.UDPPort > 0 {
				if err := rt.startVL1Listener(ctx); err != nil {
					cancel()
					return nil, fmt.Errorf("start VL1 listener: %w", err)
				}

				// NAT-behaviour classification (RFC 5780) feeds traversal-
				// method selection and is needed on EVERY platform,
				// including proxied clouds — hole-punch decisions depend
				// on our own NAT class regardless of how the public IP is
				// sourced. Run it in a goroutine so STUN round-trips (which
				// take seconds) do not gate mesh startup; it is idempotent
				// and stores its result for later consumers.
				go rt.classifyNATBehaviour(ctx)

				// Discover public IP via STUN for cross-network UDP reachability.
				// Skip STUN on proxied platforms (Fly.io) — STUN returns the egress IP
				// which differs from the dedicated Anycast ingress IP. DNS self-resolution
				// in publishSelfReachability() provides the correct IPs.
				if !cfg.Platform.IsCloud() {
					go rt.discoverPublicIP(ctx)
				} else {
					log.Printf("[NODE] STUN skipped (cloud platform %q — DNS self-resolution provides public IPs)", cfg.Platform.Provider())
				}
			}

			// Start LAD sync + re-bootstrap in background (only if LAD storage is configured)
			if ladEnabled {
				// Goroutine 1: Continuous ledger→cache sync.
				// Streams ALL records from the ledger (starting from empty watermark)
				// and applies them to the cache as they arrive. Blocks until context canceled.
				rt.Go("lad.continuous_sync", func() {
					emptyWatermark := make(lad.CausalWatermark)
					log.Printf("[NODE] LAD continuous sync started (ledger→cache)")
					if err := rt.syncer.Sync(ctx, emptyWatermark, nil); err != nil && err != context.Canceled {
						log.Printf("[NODE] LAD continuous sync error: %v", err)
					}
					log.Printf("[NODE] LAD continuous sync stopped")
				})

				// Goroutine 2: Periodic re-bootstrap + mesh status reporting.
				reBootstrapInterval := 2 * time.Minute
				if cfg.LAD.SyncInterval > reBootstrapInterval {
					reBootstrapInterval = cfg.LAD.SyncInterval
				}

				rt.Go("lad.rebootstrap_ticker", func() {
					reBootstrapTicker := time.NewTicker(reBootstrapInterval)
					defer reBootstrapTicker.Stop()
					// NOTE: reach re-publish is driven by reach.Publisher's own
					// adaptive scheduler (STABLE / CHURNING / BOOTSTRAPPING);
					// Member re-publish is driven by startMemberWatcher's
					// fingerprint-skip + cache-presence check.

					for {
						select {
						case <-ctx.Done():
							log.Printf("[NODE] LAD re-bootstrap stopped")
							return

						case <-reBootstrapTicker.C:
							// Publish latency measurements for active peers
							rt.publishPeerLatencies(ctx)

							// Log mesh status
							cacheRecords := rt.cache.Dump()
							rt.vl1Metrics.RLock()
							sessions := rt.vl1Metrics.activeSessions
							rt.vl1Metrics.RUnlock()
							dbgNode.Printf("Mesh status: %d cache records, %d active sessions", len(cacheRecords), sessions)

							// If completely isolated (no sessions), attempt full VL1 bootstrap
							if sessions == 0 {
								log.Printf("[NODE] Re-bootstrap: no active sessions, attempting VL1 reconnection")
								if err := rt.bootstrapPeers(ctx, cfg); err != nil {
									log.Printf("[NODE] Re-bootstrap: VL1 reconnection failed: %v", err)
								}
							}

							// ConnectionManager handles peer discovery and dialing
						}
					}
				})
				log.Printf("[NODE] LAD background started (continuous sync + re-bootstrap every %v)", reBootstrapInterval)
			}
		}
	}

	// Step 7: Initialize Tursoraft databases if configured
	// NOTE: Databases are optional - applications can initialize Tursoraft directly
	// in main.go for role-specific database groups and more control.
	if cfg.Databases != nil && cfg.Databases.Enabled {
		log.Printf("[NODE] Initializing Tursoraft databases...")
		dbConfig := meshdb.ManagerConfig{
			TursoConfig: meshdb.TursoConfig{
				APIToken: cfg.Databases.TursoAPIToken,
				OrgName:  cfg.Databases.TursoOrgName,
			},
			RaftConfig: meshdb.RaftConfig{
				RaftBindAddr: cfg.Databases.RaftBindAddr,
				LocalDir:     cfg.Databases.LocalDir,
				SnapshotDir:  cfg.Databases.SnapshotDir,
			},
			Groups:       cfg.Databases.Groups,
			SyncInterval: cfg.Databases.SyncInterval,
		}
		meshDB, err := meshdb.NewManager(ctx, dbConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create Tursoraft database manager: %w", err)
		}
		rt.meshDB = meshDB
		log.Printf("[NODE] Tursoraft databases initialized (%d groups)", len(cfg.Databases.Groups))
	}

	// Step 7b: Initialize QUIC fallback transport if enabled (deprecated)
	if false { // QUIC disabled - VL1 is primary
		quicPort := 41642 // Default QUIC port

		quicCfg := quictransport.QuicTransportConfig{
			LocalNode:  rt.identity.NodeID,
			PrivateKey: rt.identity.PrivateKey,
			KeepAlive:  30 * time.Second,
			ListenAddr: fmt.Sprintf(":%d", quicPort),
		}

		quicTr, err := quictransport.NewQuicTransport(quicCfg)
		if err != nil {
			log.Printf("[NODE] Warning: QUIC transport initialization failed: %v", err)
			// Non-fatal - VL1 is the primary transport
		} else {
			// Register QUIC with the global transport registry for selector fallback
			aether.RegisterTransport(aether.ProtocolQUIC, quicTr)
			log.Printf("[NODE] QUIC fallback transport initialized (port: %d)", quicPort)

			// Start QUIC listener
			rt.Go("quic.listener", func() {
				if err := rt.runQUICListener(ctx, quicTr); err != nil && err != context.Canceled {
					log.Printf("[NODE] QUIC listener error: %v", err)
				}
			})
		}
	}

	// Step 8: Anchor Service — start if anchor keys are configured.
	// All anchor-capable nodes generate snapshots independently (no leader election).
	if rt.isAnchorCapable() {
		// Register injected external RPC domains (e.g. the HSTLES anchor
		// domain providing hstles.anchor.VerifyBootstrap) so other nodes can
		// dispatch them here. Each registrar runs against a fresh registry
		// which is then exported to the handler registry.
		if registry := rt.Registry(); registry != nil && len(rt.cfg.RegisterDomains) > 0 {
			rpcRegistry := rpc.NewRegistry()
			registered := 0
			for _, registerDomain := range rt.cfg.RegisterDomains {
				if registerDomain == nil {
					continue
				}
				if err := registerDomain(rpcRegistry); err != nil {
					log.Printf("[ANCHOR] Failed to register injected domain handlers: %v", err)
					continue
				}
				registered++
			}
			if registered > 0 {
				if err := rpcRegistry.ExportToHandlerRegistry(registry); err != nil {
					log.Printf("[ANCHOR] Failed to export domain handlers to registry: %v", err)
				} else {
					log.Printf("[ANCHOR] %d injected domain(s) registered", registered)
				}
			}
		}
		// Add "anchor" to the config roles so it's included when the endpoint
		// calls PublishHandlersToLAD — avoids the endpoint's publish overwriting
		// an earlier anchor-only publish.
		hasAnchorRole := false
		for _, r := range rt.cfg.Roles {
			if r == "anchor" { hasAnchorRole = true; break }
		}
		if !hasAnchorRole {
			rt.cfg.Roles = append(rt.cfg.Roles, "anchor")
			log.Printf("[ANCHOR] Added 'anchor' to node roles: %v", rt.cfg.Roles)
		}
		if err := rt.startAnchorService(ctx, cfg); err != nil {
			log.Printf("[ANCHOR] Failed to start anchor service: %v", err)
		} else {
			log.Printf("[ANCHOR] Anchor service started (independent snapshot generation)")
		}
	}

	log.Printf("[NODE] Node runtime initialized successfully")
	log.Printf("[NODE] Node ID: %s", identity.Short())

	// Start periodic status logger (every 5 minutes)
	rt.Go("node.status_logger", func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rt.logStatus()
			}
		}
	})

	return rt, nil
}

// NodeID returns the VL1 node identifier
func (rt *Runtime) NodeID() aether.NodeID {
	return rt.identity.NodeID
}

// Identity returns the node identity
func (rt *Runtime) Identity() *NodeIdentity {
	return rt.identity
}

// IdentityPrivateKey returns the node's ed25519 private key for signing.
func (rt *Runtime) IdentityPrivateKey() ed25519.PrivateKey {
	return rt.identity.PrivateKey
}

// RPCServer returns the RPC server instance
func (rt *Runtime) RPCServer() *RPCServer {
	return rt.rpcServer
}


// Registry returns the handler registry for direct handler registration.
// Callers should use registry.RegisterRPC(h) to register handlers that
// implement handlers.RPCHandler (the preferred binary proto path).
func (rt *Runtime) Registry() *handlers.HandlerRegistry {
	if rt.rpcServer != nil {
		return rt.rpcServer.registry
	}
	return nil
}

// AuditLogger returns the audit logger if enabled
func (rt *Runtime) AuditLogger() *audit.AuditLogger {
	return rt.auditLogger
}

// CircuitBreaker returns the circuit breaker if enabled
func (rt *Runtime) CircuitBreaker() *CircuitBreaker {
	return rt.circuitBreaker
}

// HealthCheck returns the health checker if enabled
func (rt *Runtime) HealthCheck() *HealthCheck {
	return rt.healthCheck
}

// IsHealthy returns true if the node is healthy
func (rt *Runtime) IsHealthy() bool {
	if rt.healthCheck == nil {
		return true // No health checks configured
	}
	return rt.healthCheck.IsHealthy()
}

// MeshDB returns the MeshDB manager if enabled
func (rt *Runtime) MeshDB() *meshdb.Manager {
	return rt.meshDB
}

// Database returns a database handle from MeshDB
// Returns nil if MeshDB is not enabled
func (rt *Runtime) Database(groupName, dbName string) (*sql.DB, error) {
	if rt.meshDB == nil {
		return nil, fmt.Errorf("MeshDB not enabled")
	}
	return rt.meshDB.Database(groupName, dbName)
}

// Shutdown gracefully stops all node components
func (rt *Runtime) Shutdown() error {
	var shutdownErr error

	rt.shutdownOnce.Do(func() {
		log.Println("[NODE] Shutting down mesh node runtime...")

		// Step 0: Evict our own records from the LOCAL cache only.
		// Uses "liveness-local" reason so tombstones are NOT propagated via gossip.
		// Propagating shutdown tombstones blocks the node from re-registering
		// after a rapid restart (deploy), because peers reject the new records
		// as "blocked by tombstone". Peers discover departure via reach ExpiresAt
		// (10 min) and gossip liveness timeout (16 min) instead.
		if rt.cache != nil {
			nodeID := string(rt.identity.NodeID)
			rt.cache.EvictNode(nodeID, "liveness-local")
			log.Printf("[NODE] Evicted local records for %s (non-propagating)", nodeID)
		}

		// Step 1: Stop health checks
		if rt.healthCheck != nil {
			rt.healthCheck.Stop()
		}

		// Step 1b: Stop session health monitor
		if rt.sessionHealthMonitor != nil {
			rt.sessionHealthMonitor.Stop()
		}

		// Step 1c: Stop self-health monitor
		if rt.selfHealth != nil {
			rt.selfHealth.Stop()
		}

		// Step 1d: Stop LAD cache eviction
		if rt.cache != nil {
			rt.cache.StopEviction()
		}

		// Step 1e: Stop swarm PeerPublisher BEFORE rt.cancel so the
		// publisher's run-loop selects on stopCh and exits cleanly.
		// With only rt.cancel the run-loop would still exit (ctx is
		// rtCtx) but Stop() is the publisher's own owned shutdown
		// signal and surfaces a cleaner exit log for ops.
		if rt.swarm != nil && rt.swarm.Publisher != nil {
			rt.swarm.Publisher.Stop()
		}

		// Step 2: Cancel context to stop background tasks
		rt.cancel()

		// Step 3: Wait for all goroutines to finish — includes the
		// swarm.Node engine goroutine and the PeerPublisher run-loop,
		// both registered with rt.wg by InitSwarm.
		rt.wg.Wait()

		// Step 4: Close bootstrap connections
		rt.closeBootstrapConns()

		// Step 5: Close VL1 transports via manager
		if rt.transportMgr != nil {
			if err := rt.transportMgr.Close(); err != nil {
				log.Printf("[NODE] Error closing VL1 transports: %v", err)
			}
		}

		// Step 5: Close audit logger
		if rt.auditLogger != nil {
			if err := rt.auditLogger.Close(); err != nil {
				log.Printf("[NODE] Error closing audit logger: %v", err)
			}
		}

		// Step 6: Close MeshDB if initialized
		if rt.meshDB != nil {
			if err := rt.meshDB.Shutdown(); err != nil {
				log.Printf("[NODE] Error shutting down MeshDB: %v", err)
				shutdownErr = err
			}
		}

		log.Println("[NODE] Mesh node runtime shutdown complete")
	})

	return shutdownErr
}

// Context returns the runtime context
func (rt *Runtime) Context() context.Context {
	return rt.ctx
}

// Config returns the runtime configuration
func (rt *Runtime) Config() Config {
	return rt.cfg
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Runtime.Go — unified goroutine spawn API
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//
// Runtime.Go and Runtime.GoCtx are the canonical entry points for every
// long-running goroutine spawned by mesh/node, dispatch, and domain handlers.
// Every spawn that should keep the runtime alive — accept loops, walkers,
// publishers, snapshot loops, gossip/upgrade reconcilers — MUST go through
// one of these helpers instead of bare `go fn()`.
//
// Invariants the helpers guarantee:
//
//   1. wg enrollment. rt.wg.Add(1) before spawn, rt.wg.Done() on exit. The
//      Shutdown sequence (`rt.cancel(); rt.wg.Wait()`) therefore drains
//      every helper-spawned goroutine; bare `go fn()` callers are invisible
//      to wg.Wait and may outlive Close, corrupting the next Initialize on
//      hot reload.
//
//   2. Panic recovery. Each goroutine runs under a deferred recover that
//      logs the spawn site name + stack trace and returns. A panic in one
//      walker no longer kills the whole process — the mesh keeps running
//      and the operator sees `[runtime.Go] panic in <name>: ...` in logs
//      with full forensic context. This complements BidiRPC's per-request
//      recovery and the existing safeGo() helper in concurrency_helpers.go;
//      Runtime.Go is the wg-enrolled strict superset.
//
//   3. Named forensics. The `name` argument is a short identifier (e.g.
//      "lad.continuous_sync", "swarm.peer_publisher.run", "anchor.snapshot_loop")
//      that appears in panic logs and may be exposed later via a debug
//      endpoint. Use dotted-namespace strings; keep them stable so log
//      greppers don't break.
//
// GoCtx variant: passes rt.ctx to fn. Use it for goroutines whose entire
// lifetime should track the runtime — the most common shape. The plain Go
// variant is for goroutines that capture their own context (e.g. a
// per-session ctx derived from a stream).
//
// Migration is incremental. ~50 spawn sites exist across mesh/node +
// dispatch + domain; the audit-r2 plan migrates the highest-blast-radius
// sites first (accept loops, publishers, walkers). Bare `go fn()` calls
// remain valid for short-lived spawns that complete before Shutdown is
// reachable (Init-time discovery, one-shot helpers) — see RUNTIME_README
// for the exclusion list.
func (rt *Runtime) Go(name string, fn func()) {
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[runtime.Go] panic in %s: %v\n%s", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}

// GoCtx is the context-flavoured variant of Go. fn receives rt.ctx so the
// callsite doesn't have to capture the Runtime pointer just to plumb the
// shutdown signal in. Same wg-enrollment + panic-recovery semantics as Go.
func (rt *Runtime) GoCtx(name string, fn func(context.Context)) {
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[runtime.Go] panic in %s: %v\n%s", name, r, debug.Stack())
			}
		}()
		fn(rt.ctx)
	}()
}

// TransportManager returns the multi-tenant transport manager if VL1 is enabled.
func (rt *Runtime) TransportManager() *manager.TransportManager {
	return rt.transportMgr
}

// Transport returns a single aether.Transport for backward-compat callers
// (HSTLES endpoints that use single-tenant mode). In multi-tenant mode this
// returns the first listen aether. Returns nil if VL1 is not enabled.
//
// New callers should use TransportManager() for tenant-aware routing.
func (rt *Runtime) Transport() aether.Transport {
	return rt.DefaultTransport()
}

// DefaultTransport returns the first listen transport for callers that need a
// single aether.Transport (e.g., STUN discovery, legacy single-tenant).
// Returns nil if VL1 is not enabled.
func (rt *Runtime) DefaultTransport() aether.Transport {
	if len(rt.listenTransports) > 0 {
		return rt.listenTransports[0]
	}
	return nil
}

// GetPeerLatencies returns the measured RTT for each active VL1 peer session.
func (rt *Runtime) GetPeerLatencies() map[string]time.Duration {
	result := make(map[string]time.Duration)
	for _, tr := range rt.listenTransports {
		if nt, ok := tr.(*noise.NoiseTransport); ok {
			for _, pr := range nt.ActivePeerLatencies() {
				result[string(pr.NodeID)] = pr.RTT
			}
		}
	}
	return result
}

// Directory returns the LAD directory cache if enabled
func (rt *Runtime) Directory() *ladcache.DirectoryCache {
	return rt.cache
}

// LADSnapshot returns the latest typed snapshot of LAD directory state.
// Always non-nil; observability handlers should read from here instead of
// calling Directory().Members/Reach/Roles/Latency/GossipLiveness on the
// request path. The first call before warm-up returns an empty snapshot
// with Warm=false — handlers should still serve from it gracefully.
//
// The snapshot is refreshed every LADSnapshotCacheConfig.RefreshInterval
// (default 10s) by a single background goroutine; reads are lock-free
// via atomic.Pointer.
func (rt *Runtime) LADSnapshot() *LADSnapshot {
	if rt.ladSnapshot == nil {
		return &LADSnapshot{BuiltAt: time.Now()}
	}
	return rt.ladSnapshot.Snapshot()
}

// LADSnapshotCache returns the underlying snapshot cache (or nil if
// disabled / pre-init). Exposed for components that need to call Stop()
// on shutdown.
func (rt *Runtime) LADSnapshotCache() *LADSnapshotCache {
	return rt.ladSnapshot
}

// getSession returns the session for a specific node, delegating to ConnectionManager.
func (rt *Runtime) getSession(nodeID string) (aether.Session, bool) {
	if rt.connMgr != nil {
		return rt.connMgr.GetMeshSession(nodeID)
	}
	return nil, false
}

// peerGrade returns the best active transport grade for a peer.
// Returns GradeF if peer is not connected or ConnectionManager is not active.
func (rt *Runtime) peerGrade(nodeID string) Grade {
	if rt.connMgr == nil {
		return GradeF
	}
	rt.connMgr.mu.Lock()
	defer rt.connMgr.mu.Unlock()
	peer, ok := rt.connMgr.peers[nodeID]
	if !ok {
		return GradeF
	}
	return peer.bestActiveGrade()
}

// PeerManager returns the peer connection manager (may be nil before initialized).
func (rt *Runtime) ConnManager() *ConnectionManager {
	return rt.connMgr
}

// MeshSessionCount returns the number of active mesh sessions.
func (rt *Runtime) MeshSessionCount() int {
	if rt.connMgr != nil {
		return rt.connMgr.MeshSessionCount()
	}
	return 0
}

// SessionMetrics returns monitoring data for all active mesh sessions.
// Used by mesh-debug and monitoring API endpoints.
func (rt *Runtime) SessionMetrics() []MeshSessionMetrics {
	if rt.connMgr != nil {
		return rt.connMgr.SessionMetrics()
	}
	return nil
}

// HasSession returns true if a mesh session exists for a node.
func (rt *Runtime) HasSession(nodeID string) bool {
	if rt.connMgr == nil {
		return false
	}
	_, ok := rt.connMgr.GetMeshSession(nodeID)
	return ok
}

// ConnectionReporter returns a read-only view of the mesh connection state.
// Returns NilConnectionReporter if ConnectionManager is not active.
func (rt *Runtime) ConnectionReporter() ConnectionReporter {
	if rt.connMgr == nil {
		return NilConnectionReporter{}
	}
	return NewConnectionReporter(rt.connMgr)
}

// ConnectionEventLog exposes the rolling connection-lifecycle event log
// for observability endpoints. Returns nil if ConnectionManager or its
// scaler are not yet initialized. The log holds up to 4 hours / 2000
// events of Connected / Disconnected / Upgraded / Downgraded /
// DrainStarted / DrainComplete per peer.
func (rt *Runtime) ConnectionEventLog() *ConnectionEventLog {
	if rt.connMgr == nil || rt.connMgr.scaler == nil {
		return nil
	}
	return rt.connMgr.scaler.EventLog()
}

// HealthEvaluator returns the unified health evaluator (may be nil before initialized).
func (rt *Runtime) HealthEvaluator() HealthEvaluator {
	return rt.healthEvaluator
}

// SetHealthEvaluator sets the unified health evaluator on the runtime.
// Called during endpoint initialization after the cache and reporter are ready.
// Also initializes the SelfHealthMonitor to watch for staleness.
func (rt *Runtime) SetHealthEvaluator(he HealthEvaluator) {
	rt.healthEvaluator = he

	// Initialize and start the self-health monitor now that both
	// HealthEvaluator and ConnectionReporter are available.
	reporter := rt.ConnectionReporter()
	evalCfg := DefaultHealthEvaluatorConfig()
	shm := NewSelfHealthMonitor(he, reporter, evalCfg.EvalInterval, DefaultSelfHealthMonitorConfig())
	// Wire the platform-wide health registry BEFORE Start so the very
	// first tick reflects the initial evaluator/reporter state instead
	// of a delayed post-first-tick propagation.
	shm.SetRegistry(rt.healthRegistry)
	shm.Start()
	rt.selfHealth = shm
}

// HealthRegistry returns the platform-wide subsystem health registry.
// Never returns nil for a Runtime built via Initialize — the registry
// is constructed at Runtime birth so early-boot subsystems can register
// synchronously.
//
// Callers that own a subsystem (billing webhooks, uptime runner,
// vulnerability scanner, etc.) should call rt.HealthRegistry().Mark on
// a failure and Clear on recovery, using an identifier from
// health.AllowedSubsystems() so the fail-closed gate accepts it.
func (rt *Runtime) HealthRegistry() *obshealth.Registry {
	return rt.healthRegistry
}

// SelfHealth returns the observability self-health monitor.
// Returns nil if the health evaluator has not been set yet.
func (rt *Runtime) SelfHealth() *SelfHealthMonitor {
	return rt.selfHealth
}

// TopologyRouter returns a topology-aware RPC router that prefers direct
// Grade A/B connections over TLS bootstrap fallback. Each call creates a
// fresh router; callers should cache the result.
// Returns nil if ConnectionManager is not active.
func (rt *Runtime) TopologyRouter() *TopologyRouter {
	if rt.connMgr == nil {
		return nil
	}
	return NewTopologyRouter(rt.ConnectionReporter(), rt.connMgr, rt.identity)
}

// MetricsExporter returns a new MetricsExporter wired to the runtime's
// ConnectionReporter, HealthEvaluator, SelfHealthMonitor, and ConnectionScaler.
// Each call creates a fresh exporter; callers should cache the result.
func (rt *Runtime) MetricsExporter() *MetricsExporter {
	var scaler *ConnectionScaler
	var budget *ConnectionBudget
	if rt.connMgr != nil {
		scaler = rt.connMgr.scaler
		budget = rt.connMgr.budget
	}
	return NewMetricsExporter(MetricsExporterConfig{
		Reporter:       rt.ConnectionReporter(),
		Evaluator:      rt.healthEvaluator,
		SelfHealth:     rt.selfHealth,
		Scaler:         scaler,
		Budget:         budget,
		RPCServer:      rt.rpcServer,
		HealthRegistry: rt.healthRegistry,
	})
}

// AddDiscoverer adds a discovery layer (DNS SRV, mDNS, etc.) to the runtime.
// Must be called after Initialize() but before the peer connection manager
// processes discoverers (~5s after init). Discovered peers are fed into the
// ConnectionManager scan queue on first background cycle.
func (rt *Runtime) AddDiscoverer(d discovery.Discoverer) {
	rt.discoverers = append(rt.discoverers, d)
}

// getNoiseQUICPacketConn returns the DemuxPacketConn from the Noise listener
// for QUIC multiplexing on the shared UDP port. Returns nil if unavailable.
func (rt *Runtime) getNoiseQUICPacketConn() net.PacketConn {
	// Try single-tenant transport first, then shared transport
	for _, tr := range rt.listenTransports {
		if nt, ok := tr.(interface{ QUICPacketConn() net.PacketConn }); ok {
			if pc := nt.QUICPacketConn(); pc != nil {
				return pc
			}
		}
	}
	return nil
}

// Ledger returns the LAD ledger if enabled
func (rt *Runtime) Ledger() lad.Ledger {
	return rt.ledger
}

// MeshMetrics returns current VL1/LAD health metrics including cache stats.
const (
	// saturationHighWater is the connection-budget utilization at which the
	// node starts advertising the saturation bit; saturationLowWater is where
	// it stops. The gap is hysteresis — a budget hovering at the boundary will
	// not flap the advertised tag on every republish.
	saturationHighWater = 0.90
	saturationLowWater  = 0.75
	// saturationBackoffTTL is the flat lifetime stamped on backoff_until. It is
	// re-stamped (not doubled) on each republish while still saturated, and
	// consumers treat its elapse as cleared even if the boolean tag lingers —
	// bounding staleness if the node dies while saturated.
	saturationBackoffTTL = 90 * time.Second
)

// localSaturation reports whether this node is under connection-budget pressure
// and, if so, the flat unix deadline peers should respect before reopening
// paths. It latches with hysteresis so a budget near the threshold does not
// flap the advertised tag. Lock-safe: Utilization reads an
// atomically-maintained counter, and the latch is guarded by satMu.
func (rt *Runtime) localSaturation() (saturated bool, backoffUntil int64) {
	if rt.connMgr == nil || rt.connMgr.budget == nil {
		return false, 0
	}
	u := rt.connMgr.budget.Utilization()
	rt.satMu.Lock()
	if rt.satActive {
		if u <= saturationLowWater {
			rt.satActive = false
		}
	} else {
		if u >= saturationHighWater {
			rt.satActive = true
		}
	}
	active := rt.satActive
	rt.satMu.Unlock()
	if !active {
		return false, 0
	}
	return true, time.Now().Add(saturationBackoffTTL).Unix()
}

func (rt *Runtime) MeshMetrics() map[string]interface{} {
	// Snapshot connection-manager-sourced saturation metrics BEFORE taking the
	// vl1Metrics read lock: countPeersAdvertisingBackoff takes m.mu and
	// localSaturation takes satMu, so holding vl1Metrics across either would
	// impose a vl1Metrics->m.mu ordering an m.mu holder recording metrics could
	// invert. Computing them first keeps the lock domains disjoint.
	var localSat bool
	var localBackoffUntil int64
	var peersBackoff int
	if rt.connMgr != nil {
		localSat, localBackoffUntil = rt.localSaturation()
		peersBackoff = rt.connMgr.countPeersAdvertisingBackoff()
	}

	rt.vl1Metrics.RLock()
	defer rt.vl1Metrics.RUnlock()

	m := map[string]interface{}{
		"active_sessions":      rt.vl1Metrics.activeSessions,
		"total_connections":    rt.vl1Metrics.totalConnections,
		"failed_dials":         rt.vl1Metrics.failedDials,
		"last_sync_time":       rt.vl1Metrics.lastSyncTime,
		"sync_lag_seconds":     rt.vl1Metrics.syncLag.Seconds(),
		"ledger_append_errors": rt.vl1Metrics.ledgerAppendErrors,
	}

	// LAD cache stats
	if rt.cache != nil {
		cs := rt.cache.CacheStats()
		m["lad_members"] = cs.MemberCount
		m["lad_roles"] = cs.RoleCount
		m["lad_reach"] = cs.ReachCount
		m["lad_latency"] = cs.LatencyCount
		m["lad_tombstones"] = cs.TombstoneCount
		m["lad_estimated_bytes"] = cs.TotalEstimatedBytes
		m["lad_fingerprint"] = rt.cache.Fingerprint()
	}

	// Aether session stats
	m["sessions"] = rt.MeshSessionCount()

	// Aggregate mux dropped frames from ConnectionManager
	if rt.connMgr != nil {
		m["mux_dropped_frames"] = rt.connMgr.TotalDroppedFrames()
		// Saturation observability (snapshotted above, outside this lock): our
		// own latched state + how many peers are asking us to back off. Under
		// normal fleet load every node reads local_saturated=false (budgets sit
		// well under the high-water), proving the bit is wired but dormant.
		m["local_saturated"] = localSat
		m["local_backoff_until"] = localBackoffUntil
		m["peers_advertising_backoff"] = peersBackoff
	}

	// RPC forwarding counters
	if rt.rpcForwarder != nil {
		for k, v := range rt.rpcForwarder.ForwardingStats() {
			m["rpc_forward_"+k] = v
		}
	}

	// Response cache dedup stats
	if rt.rpcServer != nil {
		hits, misses := rt.rpcServer.DedupStats()
		m["dedup_hits"] = hits
		m["dedup_misses"] = misses

		// OBS-6 dispatch_handler_latency_ms — surface the top-N busiest
		// handlers (by sample count). Keeping this bounded at 20 caps the
		// MeshMetrics map growth even when the registry holds 100+ FQNs;
		// operators wanting a specific handler's distribution can hit a
		// dedicated /api/monitoring/dispatch endpoint that calls
		// DispatchLatencyTopN with a larger N. Microseconds → milliseconds
		// at publish time so the key name (_ms) matches the unit served.
		for _, snap := range rt.rpcServer.DispatchLatencyTopN(20) {
			base := "dispatch_handler_latency_ms_"
			m[base+"p50_"+snap.Handler] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Handler] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Handler] = snap.Count
		}

		// OBS-7 bidirpc_call_latency_ms — full BidiRPC.Call client-side
		// round-trip (marshal → send → wait → correlate) tagged by
		// (transport, scope). Pairs with OBS-6: OBS-6 is the responder's
		// handler-only latency, OBS-7 adds the wire + queue time, and
		// the gap between them is network/scheduler-bound time. The
		// (transport, scope) tag set is small (~6 buckets) so every
		// active bucket gets its own key pair — no top-N filter needed.
		// Microseconds → milliseconds at publish time so the key name
		// (_ms) matches the unit served.
		for _, snap := range rt.rpcServer.BidiLatencySnapshots() {
			base := "bidirpc_latency_ms_"
			m[base+"p50_"+snap.Transport+"_"+snap.Scope] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Transport+"_"+snap.Scope] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Transport+"_"+snap.Scope] = snap.Count
		}

		// Timeout histogram — parallel to bidirpc_latency_ms above but
		// fed by RecordBidiTimeout so the success-path p50/p99 is not
		// pinned at the 30s callerRequestTTL by timed-out calls. Same
		// (transport, scope) tagging shape so dashboards can pair the
		// timeout series with the success-latency series per bucket.
		for _, snap := range rt.rpcServer.BidiTimeoutSnapshots() {
			base := "bidirpc_timeout_ms_"
			m[base+"p50_"+snap.Transport+"_"+snap.Scope] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Transport+"_"+snap.Scope] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Transport+"_"+snap.Scope] = snap.Count
		}

		// OBS-7b phase decomposition — marshal / send / wait sub-phases
		// of the aggregate above. Sum approximation: bidirpc_latency_ms
		// ≈ marshal + send + wait (modulo sub-µs pending-map mutex).
		// Attributes a 5ms p50 same-origin bidirpc to codec cost vs
		// transport stall vs peer-side dispatch delay. Same (transport,
		// scope) tagging as the aggregate so dashboards can pair the
		// four series per peer-pair lane.
		marshalSnaps, sendSnaps, waitSnaps := rt.rpcServer.BidiPhaseSnapshots()
		for _, snap := range marshalSnaps {
			base := "bidirpc_marshal_ms_"
			m[base+"p50_"+snap.Transport+"_"+snap.Scope] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Transport+"_"+snap.Scope] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Transport+"_"+snap.Scope] = snap.Count
		}
		for _, snap := range sendSnaps {
			base := "bidirpc_send_ms_"
			m[base+"p50_"+snap.Transport+"_"+snap.Scope] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Transport+"_"+snap.Scope] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Transport+"_"+snap.Scope] = snap.Count
		}
		for _, snap := range waitSnaps {
			base := "bidirpc_wait_ms_"
			m[base+"p50_"+snap.Transport+"_"+snap.Scope] = float64(snap.P50US) / 1000.0
			m[base+"p99_"+snap.Transport+"_"+snap.Scope] = float64(snap.P99US) / 1000.0
			m[base+"count_"+snap.Transport+"_"+snap.Scope] = snap.Count
		}
	}

	// IntraOrgForwarder drop counters, aggregated across every
	// NoiseTransport that has a forwarder attached. Per-reason buckets
	// surface why cross-org anycast hairpins are failing (target
	// unknown in the directory, malformed preamble, stale relay-side
	// flow, etc.) so observability can distinguish "forwarder is silent
	// because no traffic" from "forwarder is firing and dropping for
	// reason X" without enabling a debug log channel.
	var preambleMalformed, targetUnknown, innerRelayMalformed,
		opForwardNoSrc, opReplyStaleFlow, opUnknown uint64
	var crossOrgMsg1Received uint64
	// forwarderAttached/Absent disambiguate a zero cross_org_msg1_received.
	//
	// Named "attached" (not "installed") per @P-239 to match the code's own
	// vocabulary: the constructor is attachIntraOrgForwarder and it logs "VL1
	// cross-org forwarder attached to %s transport", so one grep now finds the
	// log line AND the metric. Renamed while the gauge is still unpublished —
	// loom has no tag carrying it, so there is no consumer to break. Renaming a
	// metric key AFTER it ships is a different and much worse operation.
	// The msg1 counter lives INSIDE the forwarder, and the ingress hook returns
	// early when a transport has none — so msg1==0 means either "no preamble ever
	// arrived" (the delivery-gap reading) or "nothing was listening for one".
	// Without these two counters those are indistinguishable, and the decision
	// tree below silently assumes the former. attachIntraOrgForwarder is called
	// unconditionally after every transport build (default/shared/tenant), so
	// forwarder_absent SHOULD be 0 — but that is a source reading, and this makes
	// it a runtime fact.
	var forwarderAttached, forwarderAbsent uint64
	for _, tr := range rt.listenTransports {
		var nt *noise.NoiseTransport
		switch v := tr.(type) {
		case *noise.NoiseTransport:
			nt = v
		case *manager.SharedTransport:
			nt = v.Inner()
		}
		if nt == nil {
			continue
		}
		fwd := nt.Forwarder()
		if fwd == nil {
			forwarderAbsent++
			continue
		}
		forwarderAttached++
		a, b, c, d, e, f := fwd.Drops()
		preambleMalformed += a
		targetUnknown += b
		innerRelayMalformed += c
		opForwardNoSrc += d
		opReplyStaleFlow += e
		opUnknown += f
		crossOrgMsg1Received += fwd.CrossOrgMsg1Received()
	}
	// Read these BEFORE interpreting cross_org_msg1_received: if
	// forwarder_absent > 0, a zero msg1 count proves nothing about delivery.
	m["forwarder_attached"] = forwarderAttached
	m["forwarder_absent"] = forwarderAbsent
	m["forwarder_drops_preamble_malformed"] = preambleMalformed
	m["forwarder_drops_target_unknown"] = targetUnknown
	m["forwarder_drops_inner_relay_malformed"] = innerRelayMalformed
	m["forwarder_drops_op_forward_no_src"] = opForwardNoSrc
	m["forwarder_drops_op_reply_stale_flow"] = opReplyStaleFlow
	m["forwarder_drops_op_unknown"] = opUnknown
	// M_r anycast ingress receipt counter (aether v0.0.91+). Paired with
	// the dialer's cross_org_preamble_dials_attempted, it disambiguates
	// "packet never arrived" (counter==0 with attempts>0 → fly anycast
	// or UDP/6PN delivery gap) from "arrived and downstream-dropped"
	// (counter>0 with attempts>0 and forwarder_drops_* climbing → look
	// at the per-reason drop bucket).
	//
	// ⚠ TWO PRECONDITIONS before reading counter==0 as a delivery gap —
	// both were implicit here and cost three lanes a retracted verdict:
	//  1. forwarder_absent MUST be 0. The counter lives inside the
	//     forwarder and the ingress hook returns early when a transport
	//     has none, so "nothing was listening" reads identically to
	//     "nothing arrived".
	//  2. an arriving packet must be RECOGNISED as a routing preamble.
	//     The increment sits behind IsRoutingPreamble(buf); a datagram
	//     that arrives but fails that predicate is counted nowhere, so a
	//     format/version skew also presents as counter==0.
	// Neither is a Fly problem, and both are fixable in code — so
	// counter==0 alone does NOT license an infrastructure conclusion.
	m["cross_org_msg1_received"] = crossOrgMsg1Received

	// ── Mesh session resumption: capability census + establishment volume ──
	//
	// Together with mesh_initiator_sessions these turn the resumption gap
	// described at DialAndAcceptMesh into a runtime reading. Interpret as:
	//
	//   mesh_initiator_sessions climbing  AND  mesh_resume_capable_transports > 0
	//     ⇒ we hold resume-capable transports and keep establishing initiator
	//       mesh sessions that bypass their resume path. Tickets are minted
	//       during the handshake (aether/noise/handshake.go IssueTicket) and no
	//       mesh peer can ever redeem one: pure cost, previously uncounted.
	//
	//   mesh_resume_capable_transports == 0
	//     ⇒ the gap is structural for every configured protocol, and the
	//       "wasted ticket minting" reading above does NOT hold. This is the
	//       outcome that would refute it, which is why the census is here
	//       rather than assumed.
	//
	// ⚠ The assertion is deliberately made on the transport INTERFACE VALUE, not
	// on the unwrapped *noise.NoiseTransport. NoiseTransport satisfies
	// TicketCapable statically, so unwrapping first would make this a
	// compile-time tautology that always reports "capable". Asserting on tr
	// measures what a caller holding the transport can actually reach — and
	// *manager.SharedTransport does NOT satisfy TicketCapable (it forwards
	// Dial/Listen/Close but not IssueTicket/ResumeSession), so a shared-transport
	// tenant reports INCAPABLE here even though its Dial resumes internally.
	// That divergence is the finding, and it is invisible if you unwrap.
	var resumeCapable, resumeIncapable uint64
	for _, tr := range rt.listenTransports {
		if _, ok := tr.(aether.TicketCapable); ok {
			resumeCapable++
			continue
		}
		resumeIncapable++
	}
	m["mesh_resume_capable_transports"] = resumeCapable
	m["mesh_resume_incapable_transports"] = resumeIncapable
	m["mesh_initiator_sessions"] = atomic.LoadUint64(&meshInitiatorSessions)
	m["mesh_responder_sessions"] = atomic.LoadUint64(&meshResponderSessions)

	// Send-side cross-org noise-UDP forwarder dial telemetry. Pairs with
	// the receive-side forwarder_drops_* counters above to triangulate
	// why cross-Fly-org peers stay pinned on WebSocket: zero attempts =
	// dial selection skipping the path; attempts > 0 with succeeded ~0
	// AND drops ~0 = packets vanish in transit; attempts > 0 with drops
	// growing = packets arrive but classifier or hairpin lookup drops
	// them. See ConnectionManager field documentation for the full
	// decision tree.
	if rt.connMgr != nil {
		attempted, succeeded, failed := rt.connMgr.CrossOrgPreambleStats()
		m["cross_org_preamble_dials_attempted"] = attempted
		m["cross_org_preamble_dials_succeeded"] = succeeded
		m["cross_org_preamble_dials_failed"] = failed

		// Cross-org dial funnel gates (v0.0.352). Narrow which stage drops
		// noise-UDP dials before they reach the preamble code, per the
		// Batch 5 instrumentation plan. See ConnectionManager field
		// documentation for the diagnostic decision tree.
		pickIncl, pubUDP, candEmpty, allDead, nonAny, noSuit := rt.connMgr.CrossOrgGateStats()
		m["cross_org_pickpath_noise_udp_included"] = pickIncl
		m["cross_org_bestaddr_public_udp_count"] = pubUDP
		m["cross_org_bestaddr_candidates_empty"] = candEmpty
		m["cross_org_bestaddr_all_dead"] = allDead
		m["cross_org_bestaddr_non_anycast_selected"] = nonAny
		m["cross_org_dial_no_suitable_addr"] = noSuit

		// Aether session-aggregate counters. Walks every live aether
		// session and sums SessionMetrics counter fields. Without
		// this aggregation the per-session atomics (ACK emit by
		// trigger, can-send-false-reenqueues, replay-classifier hits,
		// etc.) are only observable by holding a session pointer —
		// callers consuming /api/monitoring/mesh-debug see nothing.
		// See ConnectionManager.AggregateAetherStats for the key set.
		for k, v := range rt.connMgr.AggregateAetherStats() {
			m[k] = v
		}

		// OBS-18: multipath_failover_total — cumulative count of primary-path
		// failure → promotion events across all peer multipath managers.
		m["multipath_failover_total"] = rt.connMgr.multipathFailoverTotal.Load()

		// Proactive transport-grade upgrade walker counters. ticks =
		// total walker iterations since start; candidates = cumulative
		// (peer, target-proto) probe pairs identified.
		//
		// Per-stage attrition (in walker-lifecycle order):
		//   probes_started           → probeUpgrade entered (raw attempts)
		//   probes_succeeded         → handshake complete + DialAndAcceptMesh handoff fired (handshake-honest)
		//   probes_proving           → handoff installed a 60s proving window (registerMeshSession upgrade-promote branch)
		//   probes_proving_succeeded → proving window completed and new session was still the dispatch entry (the honest upgrade-stuck count)
		//   probes_proving_failed    → new session died during proving; unregisterMeshSession reverted to old session
		//
		// Healthy mesh: started ≈ succeeded ≈ proving ≈ proving_succeeded.
		// Big drop succeeded → proving means handoffs are bypassing the
		// upgrade-promote branch (same-grade reject, dedup race). Big drop
		// proving → proving_succeeded means new sessions die under load
		// before the proving window completes — the v0.0.395-class
		// noise-UDP regression where probes_succeeded grew into the
		// thousands while bestEverGrade stayed at C.
		//
		// Failure-class counters:
		//   probes_stall_cooled  = transient dial failure, retry next tick
		//   probes_escalated     = consecutive-stall escalation → long quality cooldown
		//   probes_skipped_race  = peer already upgraded by another path between snapshot and probe
		//   probes_skipped_slot  = tryDial slot cooldown
		//
		// See upgrade_walker.go for the decision tree these counters observe.
		wTicks, wCands, wStarted, wOK, wProving, wProvingOK, wProvingFail, wStall, wEsc, wRace, wSlot := rt.connMgr.WalkerStats()
		m["upgrade_walker_ticks"] = wTicks
		m["upgrade_walker_candidates"] = wCands
		m["upgrade_walker_probes_started"] = wStarted
		m["upgrade_walker_probes_succeeded"] = wOK
		m["upgrade_walker_probes_proving"] = wProving
		m["upgrade_walker_probes_proving_succeeded"] = wProvingOK
		m["upgrade_walker_probes_proving_failed"] = wProvingFail
		m["upgrade_walker_probes_stall_cooled"] = wStall
		m["upgrade_walker_probes_escalated"] = wEsc
		m["upgrade_walker_probes_skipped_race"] = wRace
		m["upgrade_walker_probes_skipped_slot"] = wSlot

		// Role-affinity scoring telemetry. ticks = total Rebalance runs;
		// per-tier hit counters tally how many per-peer observations fired
		// each affinity tier (carry-any / same-region / dispatch-target);
		// total_bonus = cumulative K bump applied across all peers. Watch
		// the ratio dispatch_target_hits / rebalance_ticks to see how
		// often the local node sees a co-regional unique-capability peer
		// it would otherwise underconnect to.
		raTicks, raAny, raSame, raDisp, raBonus := rt.connMgr.roleAffinityStat.snapshot()
		m["role_affinity_rebalance_ticks"] = raTicks
		m["role_affinity_carry_any_hits"] = raAny
		m["role_affinity_same_region_hits"] = raSame
		m["role_affinity_dispatch_target_hits"] = raDisp
		m["role_affinity_total_bonus_applied"] = raBonus

		// unregister_skipped_not_owner is the diagnostic counter for the
		// dispatch-identity guard. Each increment is one case where an
		// obsolete cleanup goroutine would have removed a newer session
		// from dispatch and didn't, thanks to the identity check.
		// unregister_deleted is the count of cleanups that did mutate
		// dispatch. The ratio quantifies how often the guard mattered.
		m["mesh_unregister_skipped_not_owner"] = rt.connMgr.unregisterSkippedNotOwner.Load()
		m["mesh_unregister_deleted"] = rt.connMgr.unregisterDeleted.Load()

		// OBS-18: bidirpc_inflight_gauge — sum of in-flight Call() counts
		// across every BidiRPC currently registered with this node. A sustained
		// non-zero value under low traffic indicates a wedged bidi stream
		// (caller waiting for a response that will never arrive because the
		// stream has closed but the pending-channel drain path was missed).
		var bidiInflightTotal int64
		rt.connMgr.dispatchMu.RLock()
		for _, b := range rt.connMgr.bidisBySession {
			if b != nil {
				bidiInflightTotal += b.InflightCalls()
			}
		}
		rt.connMgr.dispatchMu.RUnlock()
		m["bidirpc_inflight_gauge"] = bidiInflightTotal

		// OBS-16: path_score_breakdown — one entry per (peer, proto) with the
		// most recent quality score from each multipath manager. Keyed as
		// "path_score_<peerShort>_<proto>" so the flat map structure the
		// mesh-debug surface expects doesn't require a nested-object consumer.
		// Only peers that have an active multipath manager (two or more paths)
		// emit entries; single-path peers have no PathScores snapshot to read.
		rt.connMgr.multipathMu.Lock()
		for nodeID, mgr := range rt.connMgr.multipathManagers {
			if mgr == nil {
				continue
			}
			for proto, score := range mgr.PathScores() {
				key := "path_score_" + truncID(nodeID) + "_" + proto.String()
				m[key] = score.Aggregate
			}
		}
		rt.connMgr.multipathMu.Unlock()
	}

	// ValidateSession fallback-cache hit/miss on this endpoint's
	// session meshClient (injected via Config.MeshFallbackStats). On a
	// thin gateway every cookie-bearing HTTP request calls Get(); a miss
	// pays one full BidiRPC to the auth node (~94 ms cross-org). Surfacing
	// hits + misses here lets us see whether the 30 s TTL is matching the
	// gateway's traffic pattern without scraping per-request debug logs.
	// Anchor / auth-node endpoints register a Turso-backed
	// LocalSessionStore instead of meshClient, so both counters stay at 0
	// there — and both keys are ALWAYS emitted (0 when no provider) so the
	// metrics schema is stable.
	var hits, misses uint64
	if rt.cfg.MeshFallbackStats != nil {
		hits, misses = rt.cfg.MeshFallbackStats()
	}
	m["session_validate_cache_hits"] = hits
	m["session_validate_cache_misses"] = misses

	return m
}

// logStatus logs a periodic mesh status summary to stdout.
func (rt *Runtime) logStatus() {
	rt.vl1Metrics.RLock()
	sessions := rt.vl1Metrics.activeSessions
	totalConns := rt.vl1Metrics.totalConnections
	failedDials := rt.vl1Metrics.failedDials
	rt.vl1Metrics.RUnlock()

	members := 0
	roles := 0
	if rt.cache != nil {
		if m, err := rt.cache.Members(context.Background(), ""); err == nil {
			members = len(m)
		}
		if r, err := rt.cache.Roles(context.Background(), "", ladcache.RoleQuery{}); err == nil {
			roles = len(r)
		}
	}

	dbgNode.Printf("Mesh status: node=%s sessions=%d members=%d roles=%d conns=%d failed=%d",
		rt.identity.Short(), sessions, members, roles, totalConns, failedDials)
}

// recordVL1Connection increments connection counter
func (rt *Runtime) recordVL1Connection() {
	rt.vl1Metrics.Lock()
	rt.vl1Metrics.activeSessions++
	rt.vl1Metrics.totalConnections++
	rt.vl1Metrics.Unlock()
}

// RegisterSession adds a session to the health monitor and records the connection.
func (rt *Runtime) RegisterSession(session aether.Connection) {
	rt.recordVL1Connection()
	if rt.sessionHealthMonitor != nil {
		rt.sessionHealthMonitor.Register(session)
	}
}

// UnregisterSession removes a session from the health monitor and records the disconnection.
func (rt *Runtime) UnregisterSession(nodeID aether.NodeID) {
	rt.recordVL1Disconnection()
	if rt.sessionHealthMonitor != nil {
		rt.sessionHealthMonitor.Unregister(nodeID)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// External session relay bridge (WebSocket, QUIC, gRPC)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// RegisterExternalSession adds a non-Noise session to the relay's session
// table. Used by relay endpoints that accept WebSocket or QUIC connections
// from agents when UDP is blocked.
//
// scopeID is the tenant/org scope the external session belongs to; it is
// threaded to aether's RelayCapable so the relay can enforce cross-scope
// isolation on the bridge path (aether AE-H-08). A relay that does not resolve
// a per-peer tenant passes "" (dedicated mode = no restriction), which
// preserves the pre-AE-H-08 relay behavior — the bridge path was never
// scope-checked before. Passing a non-empty scope isolates this external
// session so only same-scope (or dedicated) peers may relay to it.
func (rt *Runtime) RegisterExternalSession(nodeID aether.NodeID, sess aether.Connection, scopeID string) {
	for _, tr := range rt.listenTransports {
		if rc, ok := tr.(aether.RelayCapable); ok {
			rc.RegisterExternalSession(nodeID, sess, scopeID)
			break
		}
	}
	rt.RegisterSession(sess)
}

// UnregisterExternalSession removes a non-Noise session from the relay table.
func (rt *Runtime) UnregisterExternalSession(nodeID aether.NodeID) {
	for _, tr := range rt.listenTransports {
		if rc, ok := tr.(aether.RelayCapable); ok {
			rc.UnregisterExternalSession(nodeID)
			break
		}
	}
	rt.UnregisterSession(nodeID)
}

// HandleRelayFrame processes a relay frame received from an external (non-Noise)
// session and forwards it to the target peer via the mesh.
func (rt *Runtime) HandleRelayFrame(sourceNodeID aether.NodeID, data []byte) {
	for _, tr := range rt.listenTransports {
		if rc, ok := tr.(aether.RelayCapable); ok {
			if err := rc.HandleExternalRelayFrame(sourceNodeID, data); err != nil {
				log.Printf("relay: external frame forward failed: source=%s err=%v", string(sourceNodeID), err)
			}
			return
		}
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Dynamic tenant management (for relay/shared endpoints)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// RegisterTenant dynamically adds a tenant to the shared aether.
// Requires multi-tenant mode (Tenants config non-empty) so that a SharedTransport exists.
func (rt *Runtime) RegisterTenant(tenantID string, keys [][]byte) error {
	if rt.sharedTransport == nil {
		return fmt.Errorf("node: no shared transport (configure Tenants for multi-tenant mode)")
	}
	tid := aether.ScopeID(tenantID)
	if err := rt.sharedTransport.RegisterTenant(tid, keys); err != nil {
		return err
	}
	return rt.transportMgr.Register(tid, rt.sharedTransport)
}

// UpdateTenantKeys replaces the network keys for an existing tenant (key rotation).
func (rt *Runtime) UpdateTenantKeys(tenantID string, keys [][]byte) error {
	if rt.sharedTransport == nil {
		return fmt.Errorf("node: no shared transport (configure Tenants for multi-tenant mode)")
	}
	return rt.sharedTransport.UpdateTenantKeys(aether.ScopeID(tenantID), keys)
}

// RemoveTenant removes a tenant from the shared transport and transport manager.
func (rt *Runtime) RemoveTenant(tenantID string) error {
	if rt.sharedTransport == nil {
		return fmt.Errorf("node: no shared transport (configure Tenants for multi-tenant mode)")
	}
	tid := aether.ScopeID(tenantID)
	if err := rt.sharedTransport.RemoveTenant(tid); err != nil {
		return err
	}
	return rt.transportMgr.Remove(tid)
}

// TenantCount returns the number of registered tenants on the shared aether.
// Returns 0 if no shared transport is configured.
func (rt *Runtime) TenantCount() int {
	if rt.sharedTransport == nil {
		return 0
	}
	return rt.sharedTransport.TenantCount()
}

// recordVL1Disconnection decrements active session counter
func (rt *Runtime) recordVL1Disconnection() {
	rt.vl1Metrics.Lock()
	if rt.vl1Metrics.activeSessions > 0 {
		rt.vl1Metrics.activeSessions--
	}
	rt.vl1Metrics.Unlock()
}

// recordVL1DialFailure increments failed dial counter
func (rt *Runtime) recordVL1DialFailure() {
	rt.vl1Metrics.Lock()
	rt.vl1Metrics.failedDials++
	rt.vl1Metrics.Unlock()
}

// recordLedgerAppendError increments ledger error counter
func (rt *Runtime) recordLedgerAppendError() {
	rt.vl1Metrics.Lock()
	rt.vl1Metrics.ledgerAppendErrors++
	rt.vl1Metrics.Unlock()
}

// directoryAdapter bridges DirectoryCache to VL1 Directory interface
// The issue is that DirectoryCache.Reach uses string for tenant, but VL1 Directory wants aether.ScopeID
type directoryAdapter struct {
	cache *ladcache.DirectoryCache
}

func (d *directoryAdapter) Reach(ctx context.Context, tenant aether.ScopeID, filter lad.ReachQuery) ([]lad.ReachRecord, error) {
	// Convert aether.ScopeID (which is just a string type) to string
	// Convert lad.ReachQuery to cache.ReachQuery (cache query is simpler, lacks MinScore)
	cacheQuery := ladcache.ReachQuery{
		NodeID: filter.NodeID,
		Role:   filter.Role,
		Limit:  filter.Limit,
	}
	return d.cache.Reach(ctx, string(tenant), cacheQuery)
}

// bootstrapFromAnchor attempts to fetch and verify a directory snapshot from configured Anchor Nodes.
// Returns true if a snapshot was successfully applied with >0 records (meaning we have peers).
func (rt *Runtime) bootstrapFromAnchor(ctx context.Context, cfg Config) (bool, error) {
	// Combine explicit Anchor URLs with LAD Bootstrap Hosts
	// This ensures we can bootstrap from static gateways even if they aren't explicitly listed as anchors
	candidateURLs := make([]string, 0)
	seen := make(map[string]bool)

	// Add LAD bootstrap hosts as candidates
	if cfg.LAD.BootstrapHosts != "" {
		hosts := strings.Split(cfg.LAD.BootstrapHosts, ",")
		for _, host := range hosts {
			host = strings.TrimSpace(host)
			if host == "" {
				continue
			}
			// Normalize host to URL
			url := host
			if !strings.HasPrefix(url, "http") {
				url = "https://" + url
			}
			if !seen[url] {
				candidateURLs = append(candidateURLs, url)
				seen[url] = true
			}
		}
	}

	if len(candidateURLs) == 0 {
		return false, nil // No candidates, skip
	}

	if cfg.Anchor.PublicKey == "" {
		log.Printf("[NODE] Anchor candidates available but no PublicKey provided. Skipping secure bootstrap.")
		return false, nil
	}

	pubKey, err := hex.DecodeString(cfg.Anchor.PublicKey)
	if err != nil {
		return false, fmt.Errorf("invalid anchor public key: %w", err)
	}

	log.Printf("[NODE] Bootstrap: Attempting to fetch directory snapshot from %d candidates", len(candidateURLs))

	var snapshot *anchor.Snapshot
	var fetchErr error

	client := &http.Client{Timeout: 10 * time.Second}

	for _, url := range candidateURLs {
		// Ensure URL has scheme
		if !strings.HasPrefix(url, "http") {
			url = "https://" + url
		}

		// Append snapshot path if not present (assuming standard path)
		snapshotURL := fmt.Sprintf("%s/mesh/snapshot", strings.TrimRight(url, "/"))

		log.Printf("[NODE] Fetching snapshot from %s...", snapshotURL)

		req, err := http.NewRequestWithContext(ctx, "GET", snapshotURL, nil)
		if err != nil {
			fetchErr = err
			continue
		}

		// Secure Snapshot: Add authentication header using first available network key.
		// AuthKeys() returns global keys first, then tenant keys.
		if authKeys := cfg.AuthKeys(); len(authKeys) > 0 {
			keyBytes, err := hex.DecodeString(authKeys[0])
			if err == nil {
				token := generateAuthToken(keyBytes)
				req.Header.Set("X-Mesh-Auth", token)
			} else {
				log.Printf("[NODE] Warning: Failed to decode network key for snapshot auth: %v", err)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[NODE] Failed to fetch from %s: %v", url, err)
			fetchErr = err
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("[NODE] Anchor %s returned status %d", url, resp.StatusCode)
			fetchErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fetchErr = err
			continue
		}

		var s anchor.Snapshot
		if err := json.Unmarshal(body, &s); err != nil {
			log.Printf("[NODE] Failed to parse snapshot from %s: %v", url, err)
			fetchErr = err
			continue
		}

		// Verify signature
		if err := anchor.Verify(&s, pubKey); err != nil {
			log.Printf("[NODE] Snapshot verification failed for %s: %v", url, err)
			fetchErr = err
			continue
		}

		log.Printf("[NODE] Successfully verified snapshot from %s (Seq: %d, TS: %d)", url, s.Header.Sequence, s.Header.Timestamp)
		snapshot = &s
		break
	}

	if snapshot == nil {
		if fetchErr != nil {
			return false, fmt.Errorf("failed to fetch valid snapshot: %w", fetchErr)
		}
		return false, fmt.Errorf("failed to fetch valid snapshot from any anchor")
	}

	// Apply snapshot to directory cache
	count := 0
	for _, record := range snapshot.Records {
		if err := rt.cache.Apply(record); err != nil {
			log.Printf("[NODE] Warning: Failed to apply record from snapshot: %v", err)
			continue
		}
		count++
	}

	log.Printf("[NODE] Bootstrap: Loaded %d records from trusted snapshot", count)
	return count > 0, nil
}

// bootstrapPeers optionally dials configured bootstrap hosts to seed
// outbound VL1 sessions, then ALWAYS spawns the post-init ConnectionManager
// wiring goroutine (peer-store load, discovery layers, scan-loop Start).
//
// The dial-out phase is gated on cfg.LAD.BootstrapHosts because nodes that
// are bootstrap anchors themselves (auth core, etc.) have no outbound
// targets to seed. The scan loop is NOT gated — it is the sole driver of
// outbound mesh connection establishment now that the legacy rumorPusher
// pump is gone, and it must run on every node so that incoming sessions,
// persistent peer-store entries, discovery-layer results, and future swarm
// RoleTable lookups can all flow through it.
//
// Callers: Initialize (first init) and the LAD re-bootstrap goroutine
// (no active sessions). Re-bootstrap reuses the already-running scan loop
// and only feeds it the freshly-discovered bootstrap peers.
func (rt *Runtime) bootstrapPeers(ctx context.Context, cfg Config) error {
	type bootstrapPeerInfo struct {
		nodeID string
		host   string
		region string
	}
	var bootstrapPeers []bootstrapPeerInfo

	if cfg.LAD.BootstrapHosts != "" {
		hosts := strings.Split(cfg.LAD.BootstrapHosts, ",")
		log.Printf("[NODE] Bootstrap: Establishing VL1 connections to %d hosts via HTTPS:443: %v", len(hosts), hosts)

		for _, host := range hosts {
			host = strings.TrimSpace(host)
			if host == "" {
				continue
			}

			log.Printf("[NODE] Bootstrap: Connecting to %s via HTTPS-multiplexed VL1", host)
			result, err := rt.dialBootstrapHost(ctx, host)
			if err != nil {
				log.Printf("[NODE] Bootstrap: Failed to connect to %s: %v", host, err)
				continue
			}

			log.Printf("[NODE] Bootstrap: VL1 session established with %s (node %s)", host, result.nodeID)

			region := ""
			if result.resp != nil {
				region = result.resp.Header.Get("X-VL1-Region")
			}
			bootstrapPeers = append(bootstrapPeers, bootstrapPeerInfo{
				nodeID: result.nodeID,
				host:   host,
				region: region,
			})

			result.resp.Body.Close()
			if result.rawConn != nil {
				result.rawConn.Close()
			}
			log.Printf("[NODE] Bootstrap: %s discovery complete, connection closed (ConnectionManager will re-dial)", host)
		}

		if len(bootstrapPeers) > 0 {
			log.Printf("[NODE] Bootstrap: Discovery phase complete (%d peers found)", len(bootstrapPeers))
		} else {
			log.Printf("[NODE] Bootstrap: No bootstrap sessions established — ConnectionManager will rely on incoming sessions + peer-store + discovery layers")
		}
	} else {
		log.Printf("[NODE] No bootstrap hosts configured — ConnectionManager will rely on incoming sessions + peer-store + discovery layers")
	}

	// rt.connMgr is allocated synchronously in Initialize before any
	// listener can fire; if it is nil here something upstream failed.
	if rt.connMgr == nil {
		log.Printf("[NODE] CRITICAL: bootstrapPeers reached without connMgr allocated; skipping post-init wiring")
		return nil
	}

	// Re-bootstrap path: the post-init goroutine and the scan loop have
	// already been launched on a previous call. Just feed the existing
	// manager the freshly-discovered bootstrap peers; its scan loop will
	// re-dial them.
	if rt.connMgrPostInitDone {
		for _, bp := range bootstrapPeers {
			rt.connMgr.SeedBootstrapPeer(bp.nodeID, bp.host, bp.region)
		}
		return nil
	}
	rt.connMgrPostInitDone = true

	// Spawn the long-running ConnectionManager scan loop UNCONDITIONALLY.
	// The legacy rumorPusher implicitly drove peer discovery + dial seeding;
	// with that path retired the scan loop is the sole driver of outbound
	// mesh connection establishment. Gating it on bootstrap success would
	// leave nodes that do not dial out (auth core, anchors, isolated tenants)
	// without an active connection manager — no outbound dialing, no mesh
	// convergence, no RPC dispatch.
	rt.Go("connmgr.scan_loop", func() {
		// Handles only the post-init work that can run async: seeding
		// peers, peer store load, optional discovery layers, and the
		// long-running scan loop.

		for _, bp := range bootstrapPeers {
			rt.connMgr.SeedBootstrapPeer(bp.nodeID, bp.host, bp.region)
		}

		// Initialize persisted peer store for cold restart.
		//
		// Only enable when the node is explicitly using persistent LAD storage.
		// Nodes configured for "memory" LAD (e.g. bootstrap anchors with no
		// mounted Fly volume) have no durable filesystem — writing a peer
		// list there would churn against the container's ephemeral layer and
		// produce "rename ... no such file or directory" errors without ever
		// providing the cold-restart benefit the file is meant for.
		persistentPeerStore := rt.cfg.DataDir != "" &&
			rt.cfg.LAD.Storage != "" && rt.cfg.LAD.Storage != "memory"
		if persistentPeerStore {
			peerStore := NewFilePeerStore(string(rt.identity.NodeID), DefaultPeerStoreConfig(rt.cfg.DataDir))
			rt.peerStore = peerStore
			rt.connMgr.peerStore = peerStore

			// Load persisted peers and feed them to the scan queue
			if persistedPeers, err := peerStore.Load(); err != nil {
				log.Printf("[NODE] Failed to load persisted peers: %v", err)
			} else if len(persistedPeers) > 0 {
				log.Printf("[NODE] Feeding %d persisted peers to connection manager", len(persistedPeers))
				rt.connMgr.mu.Lock()
				for _, pp := range persistedPeers {
					if pp.NodeID == string(rt.identity.NodeID) {
						continue
					}
					if _, exists := rt.connMgr.peers[pp.NodeID]; !exists {
						rt.connMgr.peers[pp.NodeID] = &peerConn{
							nodeID:         pp.NodeID,
							state:          PeerDiscovered,
							discoveredAt:   time.Now(),
							reconnectDelay: baseCooldown,
							peerRegion:     pp.Region,
						}
					}
				}
				rt.connMgr.mu.Unlock()
			}

			// Start periodic save (every 5 min + final save on shutdown)
			peerStore.StartPeriodicSave(ctx)
		}

		// Run optional discovery layers (DNS SRV, mDNS, etc.) and feed
		// discovered peers into the ConnectionManager scan queue.
		if len(rt.discoverers) > 0 {
			var layers []discovery.DiscovererOption
			for i, d := range rt.discoverers {
				name := fmt.Sprintf("layer-%d", i)
				layers = append(layers, discovery.WithLayer(name, d))
			}
			multi := discovery.NewMultiDiscoverer(layers...)
			discovered, err := multi.Discover(ctx)
			if err != nil {
				log.Printf("[NODE] Discovery chain error: %v", err)
			} else if len(discovered) > 0 {
				log.Printf("[NODE] Feeding %d discovered peers to connection manager", len(discovered))
				rt.connMgr.mu.Lock()
				for _, dp := range discovered {
					nodeID := dp.NodeID
					if nodeID == "" || nodeID == string(rt.identity.NodeID) {
						continue
					}
					if _, exists := rt.connMgr.peers[nodeID]; !exists {
						rt.connMgr.peers[nodeID] = &peerConn{
							nodeID:         nodeID,
							state:          PeerDiscovered,
							discoveredAt:   time.Now(),
							reconnectDelay: baseCooldown,
						}
					}
				}
				rt.connMgr.mu.Unlock()
			}
		}

		// Convergence is folded into the scan loop's 60s ticker.
		rt.connMgr.Start(ctx)
	})

	return nil
}

// transitionToUDP manages the lifecycle of gossip connections:
// 1. Start HTTPS gossip to bootstrap hosts
func (rt *Runtime) dialNoiseUDP(ctx context.Context, target aether.Target) (aether.Connection, error) {
	if rt.transportMgr == nil {
		return nil, fmt.Errorf("no transport manager")
	}

	// Dial with proper tenant isolation. SharedTransport requires tenant
	// context for PSK key selection and preamble encoding.
	var lastErr error
	for i, tr := range rt.listenTransports {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		// Inject tenant context if this transport requires it
		if _, ok := tr.(*manager.SharedTransport); ok {
			tenantID := ""
			if i < len(rt.listenTenantIDs) && rt.listenTenantIDs[i] != "" {
				tenantID = rt.listenTenantIDs[i]
			}
			// If tenant ID is empty (shared transport resolves from session),
			// use the first configured tenant from the transport manager
			if tenantID == "" && len(rt.cfg.Tenants) > 0 {
				tenantID = rt.cfg.Tenants[0].TenantID
			}
			if tenantID != "" {
				dialCtx = manager.WithTenantID(dialCtx, aether.ScopeID(tenantID))
			}
		}

		session, err := tr.Dial(dialCtx, target)
		cancel()
		if err == nil {
			rt.recordVL1Connection()
			return session, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("all transports failed to dial %s: %v", target.Address, lastErr)
}

// closeBootstrapConns closes all hijacked bootstrap connections accepted by server
func (rt *Runtime) closeBootstrapConns() {
	rt.bootstrapConnsMu.Lock()
	defer rt.bootstrapConnsMu.Unlock()
	for k, c := range rt.bootstrapConns {
		if c != nil {
			if err := c.Close(); err != nil {
				log.Printf("[NODE] Error closing bootstrap conn %s: %v", k, err)
			} else {
				log.Printf("[NODE] Closed bootstrap conn %s", k)
			}
		}
		delete(rt.bootstrapConns, k)
	}
}

// startVL1Listener starts UDP listeners for all registered transports.
func (rt *Runtime) startVL1Listener(ctx context.Context) error {
	for i, tr := range rt.listenTransports {
		listener, err := tr.Listen(ctx)
		if err != nil {
			return fmt.Errorf("start UDP listener [%d]: %w", i, err)
		}

		log.Printf("[NODE] VL1 UDP listener started on %s", listener.Addr())

		// Resolve fallback tenant for this listener.
		// "" means tenant is resolved from session preamble (shared transport).
		fallbackTenant := ""
		if i < len(rt.listenTenantIDs) {
			fallbackTenant = rt.listenTenantIDs[i]
		}

		// Accept incoming sessions via Aether negotiation.
		// All sessions go through AcceptIncomingMeshSession for full Aether lifecycle.
		// Derive transport type from the listener for latency records
		transportType := "vl1" // default for VL1 listeners (typically noise-udp)
		l, tenant, tt := listener, fallbackTenant, transportType
		rt.Go("vl1.accept_loop", func() {
			rt.acceptIncomingSessionsWithMux(ctx, l, tenant, tt)
		})
	}

	return nil
}

// prefixedConn wraps a net.Conn and prepends previously-read bytes to the read stream.
// Used when we peek the first byte for mux detection and need to put it back.
type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// acceptIncomingSessionsWithMux accepts incoming VL1 sessions and sets up Aether.
// All sessions go through AcceptIncomingMeshSession for full Aether lifecycle.
// The "WithMux" name is retained for backward compatibility with call sites.
func (rt *Runtime) acceptIncomingSessionsWithMux(ctx context.Context, listener aether.Listener, fallbackTenantID string, transportType string) {
	log.Printf("[NODE] Listening for incoming Aether sessions (tenant fallback: %s)...", fallbackTenantID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			session, err := listener.Accept(ctx)
			if err != nil {
				// MESH-A02: only exit on a definitively-terminated listener
				// (context cancellation). A transient per-accept error — a
				// handshake/lifecycle failure of the churn class — must NOT kill
				// the only accept goroutine for this transport, or the node stops
				// accepting ALL future inbound sessions until restart. Mirror the
				// QUIC accept loop, which continues on the same error class.
				if ctx.Err() != nil || err == context.Canceled {
					return
				}
				if err == io.EOF {
					log.Printf("[NODE] Accept returned EOF; listener closed")
					return
				}
				log.Printf("[NODE] Accept error (transient, continuing): %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}

			tenantID := fallbackTenantID
			if tas, ok := session.(aether.TenantAwareSession); ok {
				if tid := tas.ScopeID(); tid != "" {
					tenantID = tid
				}
			}

			sessionCtx := handlers.WithTransportTenant(ctx, tenantID)

			go func(conn aether.Connection, sCtx context.Context) {
				defer func() {
					conn.Close()
					log.Printf("[NODE] Connection closed from %s", conn.RemoteNodeID().Short())
				}()

				netConn := conn.NetConn()
				nodeID := aether.NormalizeNodeID(string(conn.RemoteNodeID()))

				// Aether — all connections use Aether wire protocol.
				// Noise-UDP doesn't expose a service name in its handshake
				// frame, so the peer's Member record propagates via gossip.
				if rt.connMgr != nil {
					log.Printf("[AETHER] incoming connection from %s (proto=%s)", truncID(nodeID), transportType)
					rt.AcceptIncomingMeshSession(sCtx, netConn, nodeID, ParseProtocol(transportType), "", "", "", "")
				} else {
					// No ConnectionManager — serve as raw RPC (early boot)
					rt.rpcServer.ServeSession(sCtx, conn)
				}
			}(session, sessionCtx)
		}
	}
}

// serveRawRPC serves a session as raw binary TLV RPC (no mux).
func (rt *Runtime) serveRawRPC(ctx context.Context, sess aether.Connection) {
	defer sess.Close()
	if rt.rpcServer == nil {
		return
	}
	if err := rt.rpcServer.ServeSession(ctx, sess); err != nil {
		if err != context.Canceled && err != io.EOF {
			log.Printf("[RPC] Session error from %s: %v", sess.RemoteNodeID().Short(), err)
		}
	}
}

// runIncomingGossip was removed — AcceptMeshConnection owns gossip lifecycle for all paths.

// runQUICListener runs the QUIC listener for fallback transport
func (rt *Runtime) runQUICListener(ctx context.Context, quicTr *quictransport.QuicTransport) error {
	listener, err := quicTr.Listen(ctx)
	if err != nil {
		return fmt.Errorf("start QUIC listener: %w", err)
	}
	defer listener.Close()

	log.Printf("[NODE] QUIC listener started on %s", listener.Addr())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		session, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[NODE] QUIC accept error: %v", err)
			continue
		}

		// Register session with health monitor
		rt.RegisterSession(session)

		// Handle session via Aether — same path as all other transports
		go func(s aether.Connection) {
			defer func() {
				rt.UnregisterSession(s.RemoteNodeID())
				s.Close()
			}()

			nodeID := string(s.RemoteNodeID())
			conn := s.(aether.Connection).NetConn()
			log.Printf("[NODE] QUIC session accepted from %s, negotiating Aether", s.RemoteNodeID().Short())
			// QUIC handshake carries no service-name claim, so let gossip
			// propagate the peer's Member record.
			rt.AcceptIncomingMeshSession(ctx, conn, nodeID, ProtoQUIC, "", "", "", "")
		}(session)
	}
}

// PublishHandlersToLAD publishes handler metadata to LAD for service discovery
// This enables remote nodes to query auth requirements before routing
func (rt *Runtime) PublishHandlersToLAD(ctx context.Context, roles []string, registry *handlers.HandlerRegistry) error {
	if rt.ledger == nil {
		return fmt.Errorf("LAD ledger not initialized")
	}

	if registry == nil {
		return fmt.Errorf("handler registry not provided")
	}

	// Auto-include "anchor" role if this node is anchor-capable
	// (anchor handlers were registered during Initialize)
	if rt.isAnchorCapable() {
		hasAnchor := false
		for _, r := range roles {
			if r == "anchor" { hasAnchor = true; break }
		}
		if !hasAnchor {
			roles = append(roles, "anchor")
		}
	}

	// Collect handler metadata from registry
	handlers := registry.Handlers()
	handlerMetadata := make([]lad.HandlerMetadata, 0, len(handlers))

	for _, handler := range handlers {
		metadata := lad.HandlerMetadata{
			Name:           handler.Name(),
			RequiresAuth:   handler.RequiresAuth(),
			RequiredScopes: handler.Scopes(),
			TimeoutMs:      5000, // Default 5 second timeout
		}
		handlerMetadata = append(handlerMetadata, metadata)
	}

	// Create role record with handler metadata
	// Note: RoleRecord.TenantID will be omitted (HSTLES has no multi-tenancy)
	roleRec := lad.RoleRecord{
		NodeID:   string(rt.identity.NodeID),
		Roles:    roles,
		Handlers: handlerMetadata,
		Updated:  time.Now().UTC(),
	}

	// Marshal to JSON
	roleJSON, err := json.Marshal(roleRec)
	if err != nil {
		return fmt.Errorf("marshal role record: %w", err)
	}

	// Publish to ledger
	// Note: Record.TenantID will be omitted (HSTLES has no multi-tenancy)
	rec := lad.Record{
		Topic:        lad.TopicRole,
		NodeID:       string(rt.identity.NodeID),
		Body:         json.RawMessage(roleJSON),
		Timestamp:    time.Now().UTC(),
		LamportClock: rt.lamportClock.Tick(),
	}

	if err := rt.ledger.Append(ctx, rec); err != nil {
		rt.recordLedgerAppendError()
		return fmt.Errorf("append role record: %w", err)
	}

	dbgNode.Printf("Published %d handlers to LAD for roles %v", len(handlerMetadata), roles)
	return nil
}

// PublishRPCHandlersToLAD publishes the unified PeerRecord (roles +
// handlers + grade + capabilities + addresses) onto the swarm fleet.peer
// topic. The peer publisher signs and republishes on any input change;
// receivers index via role_table.go and address_table.go.
func (rt *Runtime) PublishRPCHandlersToLAD(ctx context.Context, reg *rpc.Registry) error {
	if reg == nil {
		return fmt.Errorf("rpc registry not provided")
	}

	roles := reg.Roles()
	if rt.isAnchorCapable() {
		hasAnchor := false
		for _, r := range roles {
			if r == "anchor" {
				hasAnchor = true
				break
			}
		}
		if !hasAnchor {
			roles = append(roles, "anchor")
		}
	}

	bestGrade := GradeC
	if rt.connMgr != nil {
		if best := rt.connMgr.BestActiveGrade(); best.BetterThan(bestGrade) {
			bestGrade = best
		}
	}

	// InitSwarm is idempotent: Runtime.Initialize already wired the
	// swarm (with reg=nil) so the route engine could come up as an
	// invariant on every node. This call returns the existing
	// integration; thread our registry through PeerPublisher.SetRegistry
	// so the handler list in subsequent PeerRecords is populated. If
	// Initialize hit the InitSwarm-failed branch (no VL1/LAD), this
	// call constructs the integration for the first time with our reg.
	si, err := rt.InitSwarm(reg)
	if err != nil {
		return fmt.Errorf("init swarm: %w", err)
	}
	if si != nil && si.Publisher != nil {
		si.Publisher.SetRegistry(reg)
	}
	// Batch the four field updates (roles + grade + service + region)
	// into a single Publisher.Update so receivers index ONE PeerRecord
	// instead of four in rapid succession. Carry operator-facing
	// identifiers in every PeerRecord so topology builders can identify
	// peers we're not directly connected to once the legacy LAD reach
	// records stop flowing.
	si.Publisher.Update(func(s *State) {
		s.Roles = roles
		s.MaxGrade = swarmGradeFromInternal(bestGrade)
		if rt.cfg.ServiceName != "" {
			s.ServiceName = rt.cfg.ServiceName
		}
		if rt.cfg.Platform != nil {
			if region := rt.cfg.Platform.Region(); region != "" {
				s.Region = region
			}
		}
	})

	// Populate the swarm PeerRecord's address list so peers can hole-
	// punch from Grade-C WS up to Grade-A noise-UDP. Without this, the
	// AddressTable subscribers see addrs=0 in every PeerRecord and the
	// dial path falls back to the LAD reach table — which is being
	// retired. Listener wiring + grade-upgrade subscriber are installed
	// here so both fire on every subsequent state change, not just the
	// first publish.
	rt.advertiseSwarmListeners(si)
	rt.installSwarmGradeUpgradeHook(si)

	dbgNode.Printf("Published peer record via swarm (roles %v, grade %v)", roles, bestGrade)

	// Route engine is no longer wired here — Runtime.Initialize made
	// it an invariant so every node (including api.hstles, login.hstles,
	// monitoring.hstles, failover.hstles, support.hstles, orbtr.io) has
	// a live route.Router from boot, not only the subset that calls
	// PublishRPCHandlersToLAD. Anchor-vs-other transit mode is decided
	// at Initialize from cfg.Roles + isAnchorCapable(); endpoints that
	// dynamically gain the anchor role after boot can call
	// rt.Route().Router.SetTransitMode to promote. ctx is retained on
	// the signature for backward compatibility with endpoint callers and
	// for future per-call cancellation needs even though no current call
	// site below depends on it.
	_ = ctx
	return nil
}

func swarmGradeFromInternal(g Grade) swarmpbGrade {
	switch {
	case g.BetterThan(GradeA):
		return swarmHighThroughput
	case g == GradeA:
		return swarmLowLatency
	case g == GradeB, g == GradeC:
		return swarmReliable
	default:
		return swarmBestEffort
	}
}

// generateAuthToken creates a time-based HMAC token for securing snapshot requests
func generateAuthToken(networkKey []byte) string {
	timestamp := time.Now().Unix()
	h := hmac.New(sha256.New, networkKey)
	h.Write([]byte(fmt.Sprintf("mesh-snapshot:%d", timestamp)))
	signature := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("v1.%d.%s", timestamp, signature)
}

// verifyAuthToken verifies the X-Mesh-Auth header against configured network keys
func (rt *Runtime) verifyAuthToken(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return false
	}

	tsStr := parts[1]
	signature := parts[2]

	timestamp, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}

	// Check timestamp freshness (allow +/- 30 seconds)
	now := time.Now().Unix()
	if timestamp < now-30 || timestamp > now+30 {
		return false
	}

	// Verify signature against ALL available network keys (global + tenant, supports rotation)
	expectedPayload := fmt.Sprintf("mesh-snapshot:%d", timestamp)

	for _, keyHex := range rt.cfg.AuthKeys() {
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			continue
		}

		h := hmac.New(sha256.New, keyBytes)
		h.Write([]byte(expectedPayload))
		expectedSig := hex.EncodeToString(h.Sum(nil))

		if hmac.Equal([]byte(signature), []byte(expectedSig)) {
			return true
		}
	}

	return false
}

// checkSnapshotRateLimit implements a token bucket rate limiter for snapshot requests
// Returns true if request is allowed, false if rate limited
// snapshotRateLimitTTL / snapshotRateLimitMaxIPs bound the per-IP rate-limit
// map (MESH-A03 / MESH-H03). The endpoint is public and keyed on the client IP,
// so without eviction a rotating-source-IP flood grows the map without bound.
const (
	snapshotRateLimitTTL    = 10 * time.Minute
	snapshotRateLimitMaxIPs = 4096
)

func (rt *Runtime) checkSnapshotRateLimit(ip string) bool {
	rt.snapshotRateLimitMu.Lock()
	defer rt.snapshotRateLimitMu.Unlock()

	now := time.Now()
	if rt.snapshotRateLimit == nil {
		rt.snapshotRateLimit = make(map[string]*rateLimitEntry)
	}

	// MESH-A03/H03: evict idle entries so the map can't grow without bound. An
	// entry idle past the TTL has fully refilled its burst and is
	// indistinguishable from a fresh one, so dropping it costs nothing.
	for k, e := range rt.snapshotRateLimit {
		if now.Sub(e.lastUpdate) > snapshotRateLimitTTL {
			delete(rt.snapshotRateLimit, k)
		}
	}

	entry, exists := rt.snapshotRateLimit[ip]
	if !exists {
		// Once saturated with distinct live IPs, refuse new ones rather than
		// grow unbounded — a rotating-XFF flood is rate-limited, not a leak.
		// NOTE: `ip` is caller-supplied (may derive from X-Forwarded-For); the
		// caller should only trust XFF from a known proxy, else key on RemoteAddr.
		if len(rt.snapshotRateLimit) >= snapshotRateLimitMaxIPs {
			return false
		}
		entry = &rateLimitEntry{tokens: 5.0, lastUpdate: now} // Burst 5
		rt.snapshotRateLimit[ip] = entry
	}

	// Refill: 1 token per second
	elapsed := now.Sub(entry.lastUpdate).Seconds()
	entry.tokens += elapsed
	if entry.tokens > 5.0 {
		entry.tokens = 5.0
	}
	entry.lastUpdate = now

	if entry.tokens >= 1.0 {
		entry.tokens -= 1.0
		return true
	}
	return false
}

// BootstrapHandler returns an http.HandlerFunc for VL1 upgrade requests.
// Consuming applications (login.hstles.com, api.hstles.com) should register this
// on their existing HTTPS server at /mesh/aether.
//
// Example usage:
//
//	runtime, _ := node.Initialize(cfg)
//	mux.HandleFunc("/mesh/vl1", runtime.BootstrapHandler())
//	http.ListenAndServeTLS(":443", certFile, keyFile, mux)
func (rt *Runtime) BootstrapHandler() http.HandlerFunc {
	// Initialize bootstrap connection map on first call
	rt.bootstrapConnsMu.Lock()
	if rt.bootstrapConns == nil {
		rt.bootstrapConns = make(map[string]net.Conn)
	}
	rt.bootstrapConnsMu.Unlock()

	return rt.handleVL1Upgrade
}

// ReachLookupHandler returns an http.HandlerFunc that serves THIS
// NODE'S OWN signed ReachRecord body. Bootstrap clients fetch this
// right after the 101 VL1 upgrade succeeds so the anchor's signed
// reach record lands in their LAD via the same TopicReach path as
// gossip-published records — restoring the "one signed envelope"
// invariant.
//
// Security model:
//   - Restricted to the local NodeID only. Requests for any other
//     peer's reach record return 403. Information disclosure is
//     bounded to what /mesh/vl1's X-VL1-Private-IP / X-VL1-Public-IP /
//     X-VL1-Service-Name / X-VL1-Region headers already advertise on
//     bootstrap upgrade — no new attack surface beyond the existing
//     bootstrap endpoint.
//   - Mesh-wide topology enumeration (the bigger concern) is NOT
//     possible from this endpoint: every node returns only its own
//     record. To learn another peer's reach, callers must complete a
//     mesh session and consume gossip via the proper channel.
//   - No authentication required — the bootstrap caller hasn't yet
//     completed the Noise handshake at the point they fetch this.
//     The record itself is signed by the node identity so the caller
//     verifies it independently before trusting it.
//
// Register at /mesh/reach/{nodeID} on the same HTTPS server that hosts
// /mesh/vl1. Returns 200 + body for self, 403 for other peers, 404
// when self record isn't cached yet (rare — only during the first
// publish), 405 for non-GET.
//
// Replaces the legacy header-derived publishBootstrapMember path which
// wrote a Member record with no addresses, leaving anchors invisible
// to bestAddress and forcing all anchor-adjacent traffic onto WS.
func (rt *Runtime) ReachLookupHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		nodeID := strings.TrimPrefix(r.URL.Path, "/mesh/reach/")
		nodeID = strings.TrimPrefix(nodeID, "/")
		if nodeID == "" {
			http.Error(w, "missing nodeID", http.StatusBadRequest)
			return
		}

		// Security: only serve our own reach record. Lookups for other
		// peers would let a caller enumerate the mesh topology from a
		// single public hostname, which is a larger info-disclosure
		// surface than necessary. Bootstrap callers only need the
		// anchor they just dialed (== self from the anchor's POV).
		// Other peers' reach records arrive via gossip after the mesh
		// session establishes.
		if nodeID != string(rt.identity.NodeID) {
			http.Error(w, "forbidden: only the local node's reach record is served by this endpoint", http.StatusForbidden)
			return
		}

		if rt.cache == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		body := rt.cache.GetLastReachBody("", nodeID)
		if len(body) == 0 {
			// Self-heal: nudge the swarm publisher and retry briefly.
			// The publish→swarm→cache pipeline is in-process so it
			// settles in milliseconds when not contended.
			if rt.swarm != nil && rt.swarm.Publisher != nil {
				rt.swarm.Publisher.PublishNow()
				deadline := time.Now().Add(500 * time.Millisecond)
				for time.Now().Before(deadline) {
					body = rt.cache.GetLastReachBody("", nodeID)
					if len(body) > 0 {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			if len(body) == 0 {
				http.Error(w, "reach record not found", http.StatusNotFound)
				return
			}
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

// WebSocketHandler returns an HTTP handler that accepts incoming WebSocket
// connections for mesh aether. Register on thin-app endpoints at /mesh/ws.
//
// Every dial is AE-C-04-verified (VerifyMeshDial: mandatory signature,
// freshness/replay, ed25519, NodeID-derives-from-pubkey) before the WS
// upgrade; an unsigned or unverifiable dial is refused. There is no
// gossip-only fallback: that unsigned path was the AE-C-04 hole.
func (rt *Runtime) WebSocketHandler() http.HandlerFunc {
	rt.wsConnsMu.Lock()
	if rt.wsConns == nil {
		rt.wsConns = make(map[string]net.Conn)
	}
	rt.wsConnsMu.Unlock()

	return func(w http.ResponseWriter, r *http.Request) {
		// Extract the remote NodeID from the header the aether WS client sends.
		// Use the transport's exported header constant, never a hardcoded string,
		// so client and server share one source of truth for the header namespace
		// and cannot skew — a hardcoded value 400s every peer if the namespace moves.
		remoteNodeID := aether.NormalizeNodeID(r.Header.Get(wstransport.NodeIDHeader))
		if remoteNodeID == "" {
			http.Error(w, "missing "+wstransport.NodeIDHeader+" header", http.StatusBadRequest)
			return
		}
		// Opportunistic service identity — peers that came through the
		// VL1 upgrade flow before switching to /mesh/ws may forward these
		// headers, enabling immediate Member-record publish. Empty is
		// fine; peer gossip will propagate the record.
		remoteServiceName := r.Header.Get("X-VL1-Service-Name")
		remoteRegion := r.Header.Get("X-VL1-Region")
		remoteRoles := r.Header.Get("X-VL1-Roles")

		// AE-C-04: verify the dial cryptographically BEFORE the WS upgrade — a
		// mandatory signature, AE-M-16 freshness/replay, ed25519, and a NodeID that
		// derives from the verified pubkey. Reject on failure here, while an HTTP
		// status can still be written (a hijacked conn can carry none). The verified
		// NodeID supersedes the header-claimed one for every downstream use; an
		// unsigned dial (the former gossip-only fallback) is now refused, which is
		// the hole AE-C-04 closes. A node with no connection manager has no transport
		// to verify with, so it fails closed rather than admitting unverified.
		if rt.connMgr == nil {
			http.Error(w, "mesh transport unavailable", http.StatusServiceUnavailable)
			return
		}
		verifiedNodeID, _, err := rt.connMgr.wsTr.VerifyMeshDial(r)
		if err != nil {
			var ve *wstransport.VerifyMeshDialError
			if errors.As(err, &ve) {
				http.Error(w, ve.Msg, ve.Status)
			} else {
				http.Error(w, "unauthorized dial", http.StatusUnauthorized)
			}
			return
		}
		remoteNodeID = string(verifiedNodeID)

		// Upgrade to WebSocket using gobwas/ws — hijacks the HTTP connection.
		// AE-P-26: emit the aether transport's signed identity triple in the 101
		// response so the dialing peer can verify this server owns the NodeID it
		// dialed. A bare UpgradeHTTP omits them, and the aether WS dialer then
		// closes the accepted socket ("server did not present a signed identity")
		// — which starves gossip, so no 6PN UDP addresses propagate and the
		// ws→noise-UDP upgrade never fires. AcceptHeaders is aether's own emit,
		// shared so this custom handler stays byte-for-byte compatible with it.
		// (Forward-port of the library v0.0.440 fix into the loom-extracted
		// mesh/node, which was extracted pre-fix — see #P-96.)
		respHdr := rt.connMgr.wsTr.AcceptHeaders(r)
		rawConn, _, _, err := ws.HTTPUpgrader{Header: respHdr}.Upgrade(r, w)
		if err != nil {
			log.Printf("[WS] Upgrade failed from %s: %v", r.RemoteAddr, err)
			return
		}

		// Wrap with WSConn adapter (server side, 10s ping keepalive for proxy)
		conn := wstransport.NewWSConn(rawConn, true, remoteNodeID, 10*time.Second)

		// Track connection
		key := remoteNodeID
		if len(key) > 12 {
			key = key[:12]
		}
		connKey := key + "@" + r.RemoteAddr

		rt.wsConnsMu.Lock()
		rt.wsConns[connKey] = conn
		rt.wsConnsMu.Unlock()

		rt.recordVL1Connection()

		// Mark peer as alive
		if rt.cache != nil {
			rt.cache.RecordGossipSeen(remoteNodeID)
		}

		// WebSocket connections carry bidirectional gossip (LAD sync).
		// The initiating side (ConnectionManager) publishes latency as "websocket".
		// The receiving side (this handler) runs gossip and publishes as "websocket" too.
		// RPC is dispatched via existing TLS bootstrap sessions or direct UDP sessions.
		// All WebSocket connections are "websocket" (Grade B).
		// Every dial is AE-C-04-verified above, so there is no unsigned or
		// gossip-only class here; gossip flows on all (verified) WebSocket conns.
		transportLabel := "websocket"
		log.Printf("[WS] Accepted %s connection from %s (%s)", transportLabel, key, r.RemoteAddr)

		// Aether — all WS connections use Aether wire protocol
		if rt.connMgr != nil && rt.cache != nil {
			log.Printf("[AETHER] WS connection from %s", truncID(remoteNodeID))
			rt.AcceptIncomingMeshSession(rt.ctx, conn, remoteNodeID, ProtoWebSocket, "", remoteServiceName, remoteRegion, remoteRoles)
		}

		// Cleanup
		rt.wsConnsMu.Lock()
		delete(rt.wsConns, connKey)
		rt.wsConnsMu.Unlock()
		_ = conn.Close()
		rt.recordVL1Disconnection()
		log.Printf("[WS] Connection closed from %s (%s)", key, r.RemoteAddr)
	}
}

// HijackRelayHandler returns an HTTP handler that accepts incoming hijack relay
// connections at /mesh/relay. These use HTTP/1.1 Upgrade + hijack instead of
// WebSocket framing, avoiding proxy interference on Fly.io and Cloudflare.
//
// Hijack connections carry the same gossip/RPC traffic as WebSocket but use
// length-prefixed binary framing over a raw TLS stream. Register alongside
// WebSocketHandler:
//
//	mux.HandleFunc("/mesh/ws", runtime.WebSocketHandler())
//	mux.HandleFunc("/mesh/relay", runtime.HijackRelayHandler())
func (rt *Runtime) HijackRelayHandler() http.HandlerFunc {
	if rt.connMgr == nil || rt.connMgr.wsTr == nil {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "relay transport not initialized", http.StatusServiceUnavailable)
		}
	}

	handler, sessionCh := rt.connMgr.wsTr.HijackHandler()

	// Consume accepted hijack sessions in a background goroutine
	go func() {
		for {
			select {
			case <-rt.ctx.Done():
				return
			case hs, ok := <-sessionCh:
				if !ok {
					return
				}
				go rt.handleHijackSession(hs)
			}
		}
	}()

	return handler
}

// handleHijackSession runs gossip over an accepted hijack relay connection.
// The ConnectionManager owns the full lifecycle (mux, gossip, dispatch, cleanup).
func (rt *Runtime) handleHijackSession(hs wstransport.HijackSession) {
	remoteNodeID := string(hs.RemoteNodeID)

	rt.recordVL1Connection()
	defer func() {
		hs.Conn.Close()
		rt.recordVL1Disconnection()
		log.Printf("[HIJACK] Connection closed from %s", truncID(remoteNodeID))
	}()

	if rt.cache != nil {
		rt.cache.RecordGossipSeen(remoteNodeID)
	}

	log.Printf("[HIJACK] Accepted relay connection from %s", truncID(remoteNodeID))

	// Aether — all hijack connections use Aether wire protocol.
	// The hijack handshake doesn't carry a service-name claim; gossip
	// propagates the peer's Member record shortly after.
	if rt.connMgr != nil && rt.cache != nil {
		log.Printf("[AETHER] hijack connection from %s", truncID(remoteNodeID))
		rt.AcceptIncomingMeshSession(rt.ctx, hs.Conn, remoteNodeID, ProtoTLS, "", "", "", "")
	}
}

// GRPCServer returns the gRPC server for cmux integration on a shared port.
// The caller wraps the existing TCP listener with cmux:
//
//	m := cmux.New(listener)
//	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
//	httpL := m.Match(cmux.Any())
//	go grpcServer.Serve(grpcL)
//	go httpServer.Serve(httpL)
//	m.Serve()
//
// Incoming gRPC sessions are handled by the RPC server automatically
// via the stream interceptor in the gRPC aether.
func (rt *Runtime) GRPCServer() *grpc.Server {
	if rt.connMgr == nil || rt.connMgr.grpcTr == nil {
		return nil
	}
	return rt.connMgr.grpcTr.CreateServer()
}

// startGRPCAcceptLoop reads incoming gRPC sessions from the transport's channel.
// The cmux server (via CreateServer + ServeWithGRPC) feeds t.incoming when peers connect.
// No standalone listener needed — peers connect to the HTTP port via cmux.
func (rt *Runtime) startGRPCAcceptLoop(ctx context.Context) {
	if rt.connMgr == nil || rt.connMgr.grpcTr == nil {
		return
	}

	log.Printf("[NODE] gRPC accept loop started (cmux-backed)")

	rt.Go("grpc.accept_loop", func() {
		for {
			select {
			case <-ctx.Done():
				return
			case incoming, ok := <-rt.connMgr.grpcTr.Incoming():
				if !ok {
					return
				}
				sess := incoming.Session
				rt.Go("grpc.session_handler", func() {
					defer sess.Close()
					nodeID := aether.NormalizeNodeID(string(sess.RemoteNodeID()))
					conn := sess.NetConn()
					log.Printf("[AETHER] gRPC connection from %s", truncID(nodeID))
					// gRPC handshake carries no service-name metadata in
					// the current wire format; gossip propagates the peer's
					// Member record.
					rt.AcceptIncomingMeshSession(ctx, conn, nodeID, ProtoGRPC, "", "", "", "")
				})
			}
		}
	})
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Connectivity Test Diagnostic Endpoint
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// ConnectivityTestResult is the JSON response from the /mesh/connectivity-test endpoint.
type ConnectivityTestResult struct {
	NodeID    string                    `json:"nodeId"`
	Protocols map[string]ProtocolStatus `json:"protocols"`
	STUN      *STUNStatus               `json:"stun,omitempty"`
	Listeners map[string]ListenerStatus `json:"listeners"`
	Timestamp time.Time                 `json:"timestamp"`
}

// ProtocolStatus reports the state of a single transport protocol.
type ProtocolStatus struct {
	Available   bool   `json:"available"`    // is protocol registered / transport present?
	ActivePeers int    `json:"activePeers"` // number of connected peers via this protocol
	Grade       string `json:"grade"`        // A/B/C
}

// STUNStatus reports reflexive address discovery status.
type STUNStatus struct {
	Enabled       bool   `json:"enabled"`
	ReflexiveIP   string `json:"reflexiveIp,omitempty"`
	ReflexivePort int    `json:"reflexivePort,omitempty"`
	NATType       string `json:"natType,omitempty"`
	Error         string `json:"error,omitempty"`
}

// ListenerStatus reports whether a protocol listener is active.
type ListenerStatus struct {
	Listening bool   `json:"listening"`
	Address   string `json:"address,omitempty"`
}

// ConnectivityTestHandler returns an http.HandlerFunc that probes all registered
// transport protocols and reports their connectivity status. Register at /mesh/connectivity-test.
//
// By default STUN discovery is skipped (non-blocking). Pass ?stun=true to trigger
// a live STUN probe (adds up to 3s latency).
//
// Example usage:
//
//	mux.HandleFunc("/mesh/connectivity-test", runtime.ConnectivityTestHandler())
func (rt *Runtime) ConnectivityTestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := ConnectivityTestResult{
			NodeID:    string(rt.identity.NodeID),
			Protocols: make(map[string]ProtocolStatus),
			Listeners: make(map[string]ListenerStatus),
			Timestamp: time.Now().UTC(),
		}

		// ── Protocols: check ConnectionManager for per-protocol peer counts ──
		peersByProto := make(map[string]int)
		if rt.connMgr != nil {
			states := rt.connMgr.PeerStates()
			for _, info := range states {
				if info["state"] == "connected" {
					proto := info["protocol"]
					if proto != "" {
						peersByProto[proto]++
					}
				}
			}
		}

		// Report all known protocol types with their status.
		type protoEntry struct {
			name  string
			grade string
			has   bool // whether the transport is available
		}
		entries := []protoEntry{
			{"noise-udp", "A", len(rt.listenTransports) > 0},
			{"quic", "A", rt.connMgr != nil && rt.connMgr.quicTr != nil},
			{"websocket", "B", rt.connMgr != nil && rt.connMgr.wsTr != nil},
			{"grpc", "C", rt.connMgr != nil && rt.connMgr.grpcTr != nil},
			{"tls", "B", true}, // TLS bootstrap is always conceptually available
		}
		for _, e := range entries {
			result.Protocols[e.name] = ProtocolStatus{
				Available:   e.has,
				ActivePeers: peersByProto[e.name],
				Grade:       e.grade,
			}
		}

		// ── STUN: optionally probe reflexive address ──
		doSTUN := r.URL.Query().Get("stun") == "true"
		if doSTUN {
			stunStatus := &STUNStatus{}
			probed := false
			for _, tr := range rt.listenTransports {
				var nt *noise.NoiseTransport
				switch t := tr.(type) {
				case *noise.NoiseTransport:
					nt = t
				case *manager.SharedTransport:
					nt = t.Inner()
				}
				if nt == nil {
					continue
				}
				if nt.GetSTUNClient() == nil {
					stunStatus.Enabled = false
					stunStatus.Error = "STUN not enabled on transport"
					probed = true
					break
				}
				stunStatus.Enabled = true
				stunCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				reflex, err := nt.DiscoverReflexiveAddr(stunCtx, nil)
				cancel()
				if err != nil {
					stunStatus.Error = err.Error()
				} else {
					stunStatus.ReflexiveIP = reflex.IP.String()
					stunStatus.ReflexivePort = reflex.Port
					stunStatus.NATType = string(reflex.NATType)
				}
				probed = true
				break
			}
			if !probed {
				stunStatus.Enabled = false
				stunStatus.Error = "no noise transport available"
			}
			result.STUN = stunStatus
		} else if ip := rt.getPublicIP(); ip != "" {
			// Return cached STUN info without blocking
			result.STUN = &STUNStatus{
				Enabled:     true,
				ReflexiveIP: ip,
			}
		}

		// ── Listeners: report status of each transport listener ──
		// Noise/UDP listeners
		for i, tr := range rt.listenTransports {
			label := fmt.Sprintf("noise-udp-%d", i)
			switch t := tr.(type) {
			case *noise.NoiseTransport:
				if l := t.QUICPacketConn(); l != nil {
					// Listener is active (QUICPacketConn is derived from the listener socket)
					result.Listeners[label] = ListenerStatus{
						Listening: true,
						Address:   l.LocalAddr().String(),
					}
				} else {
					result.Listeners[label] = ListenerStatus{Listening: false}
				}
			case *manager.SharedTransport:
				inner := t.Inner()
				if inner != nil {
					if l := inner.QUICPacketConn(); l != nil {
						result.Listeners[label] = ListenerStatus{
							Listening: true,
							Address:   l.LocalAddr().String(),
						}
					} else {
						result.Listeners[label] = ListenerStatus{Listening: false}
					}
				}
			default:
				result.Listeners[label] = ListenerStatus{Listening: false}
			}
		}

		// QUIC listener (shares port with Noise via demux)
		if rt.connMgr != nil && rt.connMgr.quicTr != nil {
			result.Listeners["quic"] = ListenerStatus{Listening: true, Address: "shared:41641"}
		} else {
			result.Listeners["quic"] = ListenerStatus{Listening: false}
		}

		// WebSocket listener (accepts via HTTP upgrade, not a standalone listener)
		result.Listeners["websocket"] = ListenerStatus{
			Listening: true,
			Address:   "via-http-upgrade",
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache, no-store")
		json.NewEncoder(w).Encode(result)
	}
}

// wsPingLoop removed — WSConn adapter handles ping/pong at frame level via gobwas/ws.

// handleVL1Upgrade handles HTTP requests to upgrade to VL1 protocol
func (rt *Runtime) handleVL1Upgrade(w http.ResponseWriter, r *http.Request) {
	// 1. Rate Limiting (Token Bucket)
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	// Handle X-Forwarded-For if behind proxy
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		ip = strings.TrimSpace(strings.Split(fwd, ",")[0])
	}

	if !rt.checkSnapshotRateLimit(ip) {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Verify upgrade request
	if r.Header.Get("Upgrade") != "vl1" {
		http.Error(w, "VL1 upgrade required", http.StatusBadRequest)
		return
	}

	// Read client's advertised ports, addresses, and service name
	clientUDPPort := r.Header.Get("X-VL1-UDP-Port")
	clientFailoverPort := r.Header.Get("X-VL1-Failover-Port")
	clientNodeID := aether.NormalizeNodeID(r.Header.Get("X-VL1-Node-ID"))
	clientPrivateIP := r.Header.Get("X-VL1-Private-IP")
	clientPublicIP := r.Header.Get("X-VL1-Public-IP")
	clientServiceName := r.Header.Get("X-VL1-Service-Name")
	clientRegion := r.Header.Get("X-VL1-Region")
	clientRoles := r.Header.Get("X-VL1-Roles")

	log.Printf("[NODE] Bootstrap: VL1 upgrade request from %s (UDP ports: %s primary, %s failover)",
		r.RemoteAddr, clientUDPPort, clientFailoverPort)

	// Auth: anchor-capable nodes validate locally, thin apps delegate to
	// the injected verifier (the HSTLES impl performs the
	// hstles.anchor.VerifyBootstrap rpc.Call; nil = allow all).
	if !rt.isAnchorCapable() && rt.cfg.VerifyBootstrap != nil {
		var clientRolesList []string
		if clientRoles != "" {
			clientRolesList = strings.Split(clientRoles, ",")
		}
		allowed, reason, err := rt.cfg.VerifyBootstrap(r.Context(), ports.BootstrapInfo{
			NodeID:      ports.NodeID(clientNodeID),
			ServiceName: clientServiceName,
			Region:      clientRegion,
			Roles:       clientRolesList,
			PrivateIP:   clientPrivateIP,
			PublicIP:    clientPublicIP,
		})
		if err != nil {
			// Verifier unreachable — allow connection (graceful fallback).
			// Transport-level auth (Noise handshake) is the real security boundary.
			log.Printf("[NODE] Bootstrap: bootstrap verifier unavailable, allowing connection: %v", err)
		} else if !allowed {
			log.Printf("[NODE] Bootstrap: join denied: %s", reason)
			http.Error(w, reason, http.StatusForbidden)
			return
		}
		truncLen := len(clientNodeID)
		if truncLen > 12 { truncLen = 12 }
		log.Printf("[NODE] Bootstrap: join approved for %s via injected verifier", clientNodeID[:truncLen])
	}

	// Hijack the connection to take control of the underlying TCP socket.
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "server does not support hijacking", http.StatusInternalServerError)
		return
	}

	conn, rw, err := hj.Hijack()
	if err != nil {
		log.Printf("[NODE] Bootstrap: hijack failed: %v", err)
		return
	}

	// Aggressive TCP keepalive on hijacked connection
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(5 * time.Second)
	}

	// Write the 101 Switching Protocols response ourselves over the hijacked connection
	_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	_, _ = rw.WriteString("Upgrade: vl1\r\n")
	_, _ = rw.WriteString("Connection: Upgrade\r\n")
	_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Node-ID: %s\r\n", string(rt.identity.NodeID)))
	_, _ = rw.WriteString(fmt.Sprintf("X-VL1-UDP-Port: %d\r\n", rt.cfg.VL1.UDPPort))
	if rt.cfg.VL1.FailoverPort > 0 {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Failover-Port: %d\r\n", rt.cfg.VL1.FailoverPort))
	}
	// Advertise service name and region so peers can build member records
	if rt.cfg.ServiceName != "" {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Service-Name: %s\r\n", rt.cfg.ServiceName))
	}
	_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Region: %s\r\n", rt.cfg.Platform.Region()))
	if len(rt.cfg.Roles) > 0 {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Roles: %s\r\n", strings.Join(rt.cfg.Roles, ",")))
	}
	// Advertise private IP for internal mesh networking (discovered from interfaces)
	if privateIP := rt.discoverPrivateIP(); privateIP != "" {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Private-IP: %s\r\n", privateIP))
	}
	// Advertise STUN-discovered public IP for cross-network UDP
	if ip := rt.getPublicIP(); ip != "" {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Public-IP: %s\r\n", ip))
	}
	// Reflect the client's observed public IP (from X-Forwarded-For / connection source)
	// This lets the client correct its STUN-discovered IP when behind NAT/proxy
	if ip != "" && ip != "127.0.0.1" && ip != "0.0.0.0" {
		_, _ = rw.WriteString(fmt.Sprintf("X-VL1-Observed-IP: %s\r\n", ip))
	}
	_, _ = rw.WriteString("\r\n")
	if err := rw.Flush(); err != nil {
		log.Printf("[NODE] Bootstrap: failed flushing hijacked response: %v", err)
		_ = conn.Close()
		return
	}

	// Store connection so we can manage/close it later (server-side counterpart
	// to client bootstrap sessions). MESH-A05: include RemoteAddr in the key so a
	// reconnecting peer (same NodeID) gets a DISTINCT slot. Keying by NodeID
	// alone let conn2 overwrite conn1 untracked, and conn1's later goroutine-exit
	// delete then removed the live conn2 — leaving it unclosed on Shutdown. Close
	// any pre-existing conn at the same key defensively.
	key := clientNodeID + "|" + r.RemoteAddr
	if clientNodeID == "" {
		key = r.RemoteAddr
	}
	rt.bootstrapConnsMu.Lock()
	if prev, ok := rt.bootstrapConns[key]; ok && prev != conn {
		_ = prev.Close()
	}
	rt.bootstrapConns[key] = conn
	rt.bootstrapConnsMu.Unlock()

	// Record successful VL1 connection
	rt.recordVL1Connection()

	log.Printf("[NODE] Bootstrap: VL1 upgrade completed and connection hijacked for %s (key=%s, private=%s)", r.RemoteAddr, key, clientPrivateIP)

	// Create a reach record for the connecting peer so we know how to reach them
	if clientNodeID != "" && rt.ledger != nil {
		var clientPort int
		if clientUDPPort != "" {
			fmt.Sscanf(clientUDPPort, "%d", &clientPort)
		}
		if clientPort == 0 {
			clientPort = rt.cfg.VL1.UDPPort
		}

		// NOTE: Reach records are NOT created here for the remote node.
		// Each node is the sole authority on its own reach record — it publishes
		// via publishSelfReachability() with correct addresses (WSS, dedicated IPs,
		// private fdaa:). Bootstrap-created records had incomplete addresses
		// (connection source IP only, no WSS/hostname) that overwrote the
		// correct self-published records and prevented cross-org WS connections.
		_ = clientPrivateIP // used by member record below
		_ = clientPort
	}

	// Create a member record for the connecting peer so the topology API
	// can group nodes by service name. This propagates via anchor snapshots.
	if clientNodeID != "" && clientServiceName != "" && rt.ledger != nil {
		memberAttrs := map[string]string{"serviceName": clientServiceName}
		if clientRegion != "" {
			memberAttrs["region"] = clientRegion
		}
		if clientRoles != "" {
			memberAttrs["roles"] = clientRoles
		}
		memberRec := lad.MemberRecord{
			NodeID:    clientNodeID,
			CreatedAt: time.Now().UTC(),
			Attrs:     memberAttrs,
		}
		if memberJSON, err := json.Marshal(memberRec); err == nil {
			_ = rt.ledger.Append(context.Background(), lad.Record{
				Topic:        lad.TopicMember,
				NodeID:       clientNodeID,
				Body:         json.RawMessage(memberJSON),
				Timestamp:    time.Now().UTC(),
				LamportClock: rt.lamportClock.Tick(),
			})
		}
	}

	// Mark both nodes as alive in gossip liveness
	if rt.cache != nil && clientNodeID != "" {
		rt.cache.RecordGossipSeen(clientNodeID)
		rt.cache.RecordGossipSeen(string(rt.identity.NodeID))
	}

	// Run bidirectional LAD gossip over the hijacked connection.
	// Each side periodically exchanges its full cache dump with the peer.
	go func(k string, c net.Conn, peerNodeID, svcName, region, roles string) {
		defer func() {
			rt.bootstrapConnsMu.Lock()
			delete(rt.bootstrapConns, k)
			rt.bootstrapConnsMu.Unlock()
			_ = c.Close()
			rt.recordVL1Disconnection()
			log.Printf("[NODE] Bootstrap: gossip connection closed for %s (key=%s)", r.RemoteAddr, k)
		}()

		if rt.cache == nil || rt.connMgr == nil {
			// No cache or ConnectionManager — fall back to idle read
			buf := make([]byte, 1)
			for {
				_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
				if _, err := c.Read(buf); err != nil {
					return
				}
			}
		}

		// Aether — bootstrap server uses Aether wire protocol. Forward
		// the peer's advertised service identity + region (captured from
		// the VL1 upgrade headers above) so AcceptMeshConnection can
		// publish a Member record immediately AND seed peer.peerRegion
		// at accept-time for the role-affinity sameRegion bonus.
		log.Printf("[AETHER] bootstrap server connection from %s", truncID(peerNodeID))
		rt.AcceptIncomingMeshSession(rt.ctx, c, peerNodeID, ProtoTLS, "", svcName, region, roles)
	}(key, conn, clientNodeID, clientServiceName, clientRegion, clientRoles)
}

// startAnchorService initializes the anchor generator and starts the snapshot loop.
func (rt *Runtime) startAnchorService(ctx context.Context, cfg Config) error {
	if cfg.Anchor.PrivateKey == "" {
		return nil
	}

	log.Printf("[NODE] Starting Anchor Service...")

	signer, err := anchor.NewSigner(string(rt.identity.NodeID), cfg.Anchor.PrivateKey)
	if err != nil {
		return fmt.Errorf("create anchor signer: %w", err)
	}

	rt.anchorGenerator = anchor.NewGenerator(signer)

	// Start background loop
	rt.Go("anchor.snapshot_loop", func() {
		ticker := time.NewTicker(1 * time.Minute) // Generate snapshot every minute
		defer ticker.Stop()

		// Generate initial snapshot immediately
		if err := rt.generateSnapshot(); err != nil {
			log.Printf("[NODE] Error generating initial snapshot: %v", err)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := rt.generateSnapshot(); err != nil {
					log.Printf("[NODE] Error generating snapshot: %v", err)
				}
			}
		}
	})

	return nil
}

func (rt *Runtime) generateSnapshot() error {
	if rt.anchorGenerator == nil {
		return nil
	}

	records := rt.cache.Dump()

	seq := uint64(time.Now().Unix())

	snapshot, err := rt.anchorGenerator.Generate(records, seq)
	if err != nil {
		return err
	}

	rt.snapshotMu.Lock()
	rt.latestSnapshot = snapshot
	rt.snapshotMu.Unlock()

	log.Printf("[NODE] Generated new anchor snapshot (Seq: %d, Records: %d)", seq, len(records))
	return nil
}

// GetLatestSnapshot returns the most recent signed snapshot.
func (rt *Runtime) GetLatestSnapshot() *anchor.Snapshot {
	rt.snapshotMu.RLock()
	defer rt.snapshotMu.RUnlock()
	return rt.latestSnapshot
}


// isRoutablePrivateIP returns true if the IP is a private address suitable for
// inter-host mesh connectivity. Filters out container/virtual network IPs:
//   - Docker bridge: 172.16-31.x.x (RFC 1918 but not routable between hosts)
//   - Container loopback/link-local
// Prefers IPv6 ULA (fd00::/8, includes Fly fdaa:) over IPv4 RFC1918.
func isRoutablePrivateIP(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
		return false
	}
	if !ip.IsPrivate() {
		return false
	}
	// Filter Docker bridge range (172.16.0.0/12) — not routable between hosts
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
		return false
	}
	return true
}

// discoverPrivateIP returns the node's private IP for internal mesh networking.
// Uses a tiered discovery chain — first valid routable private IP wins:
//   0. PlatformInfo.PrivateIP() — authoritative on cloud (FLY_PRIVATE_IP
//      on Fly, IMDS local-ipv4 on AWS, metadata internal-ip on GCP,
//      POD_IP on k8s). Synchronous, single-shot, provider-agnostic via
//      the platform abstraction.
//   1. Hostname resolution (os.Hostname → net.LookupHost)
//   2. Interface enumeration (fd00::/8 ULA preferred)
//   3. .internal DNS lookup
//   4. Outbound route probe (dial bootstrap host, check local addr)
// Filters out Docker bridge IPs (172.16-31.x.x). Prefers IPv6 ULA (fdaa:).
// Falls back to PrivateIP config field if all tiers fail.
func (rt *Runtime) discoverPrivateIP() string {
	// Config override takes priority
	if rt.cfg.PrivateIP != "" {
		return rt.cfg.PrivateIP
	}

	// Tier 0: Platform-authoritative source. Synchronous, provider-
	// agnostic via the PlatformInfo interface — no per-platform env
	// reads here. Replaces the legacy detectOurOriginLoop polling for
	// the cross-org forwarder's ourOrigin determination.
	if rt.cfg.Platform != nil {
		if ip := rt.cfg.Platform.PrivateIP(); ip != "" {
			return ip
		}
	}

	// Tier 1: Hostname resolution — prefer IPv6 ULA over IPv4
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		if ips, err := net.LookupHost(hostname); err == nil {
			var bestIPv4 string
			for _, ip := range ips {
				parsed := net.ParseIP(ip)
				if !isRoutablePrivateIP(parsed) {
					continue
				}
				// Prefer IPv6 (overlay/mesh networks like fdaa:)
				if parsed.To4() == nil {
					return ip
				}
				if bestIPv4 == "" {
					bestIPv4 = ip
				}
			}
			if bestIPv4 != "" {
				return bestIPv4
			}
		}
	}

	// Tier 2: Interface addresses — look for IPv6 ULA (fd00::/8, includes fdaa:)
	// These are overlay/mesh network IPs that route between hosts.
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && isRoutablePrivateIP(ipnet.IP) {
				// Prefer IPv6 ULA (fdaa:, fd00:) — overlay networks
				if ipnet.IP.To4() == nil && ipnet.IP[0] == 0xfd {
					return ipnet.IP.String()
				}
			}
		}
	}

	// Tier 3: .internal DNS lookup — some platforms (e.g., overlay networks)
	// provide internal DNS that resolves to the node's private mesh address
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		internalName := hostname + ".internal"
		if ips, err := net.LookupHost(internalName); err == nil {
			for _, ip := range ips {
				parsed := net.ParseIP(ip)
				if isRoutablePrivateIP(parsed) {
					return ip
				}
			}
		}
	}

	// Tier 4: Outbound route probe — dial first bootstrap host to discover
	// which local IP the OS routing table selects for that destination
	if rt.cfg.LAD.BootstrapHosts != "" {
		hosts := strings.Split(rt.cfg.LAD.BootstrapHosts, ",")
		for _, h := range hosts {
			h = strings.TrimSpace(h)
			if h == "" {
				continue
			}
			// Ensure host:port format for UDP dial
			target := h
			if _, _, err := net.SplitHostPort(h); err != nil {
				target = net.JoinHostPort(h, "41641")
			}
			conn, err := net.DialTimeout("udp", target, 1*time.Second)
			if err != nil {
				continue
			}
			localAddr := conn.LocalAddr().(*net.UDPAddr)
			conn.Close()
			if isRoutablePrivateIP(localAddr.IP) {
				return localAddr.IP.String()
			}
		}
	}

	return ""
}

// discoverPublicIP uses STUN to discover the node's public (reflexive)
// IP address. The reflexive address is advertised to peers for direct
// cross-network reachability.
//
// This is distinct from NAT-behaviour classification (see
// classifyNATBehaviour): public-IP discovery is only meaningful on
// non-proxied platforms. On cloud providers (Fly.io) STUN returns the
// egress IP, which differs from the dedicated Anycast ingress IP, so
// the caller skips this on cloud — DNS self-resolution in
// publishSelfReachability() provides the correct IPs there.
// getPublicIP / setPublicIP guard rt.publicIP (MESH-A01). The field is written
// from the STUN discovery goroutine and the re-bootstrap ticker, and read from
// several HTTP handlers and the hole-punch candidate builder — all concurrent.
// A bare string is a two-word (ptr,len) value, so an unsynchronized read could
// tear and advertise a corrupt address.
func (rt *Runtime) getPublicIP() string {
	rt.publicIPMu.RLock()
	defer rt.publicIPMu.RUnlock()
	return rt.publicIP
}

func (rt *Runtime) setPublicIP(ip string) {
	rt.publicIPMu.Lock()
	rt.publicIP = ip
	rt.publicIPMu.Unlock()
}

func (rt *Runtime) discoverPublicIP(ctx context.Context) {
	for _, tr := range rt.listenTransports {
		if nt, ok := tr.(*noise.NoiseTransport); ok {
			stunClient := nt.GetSTUNClient()
			if stunClient == nil {
				continue
			}
			localAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", rt.cfg.VL1.UDPPort))
			reflexive, err := stunClient.DiscoverReflexiveAddr(ctx, localAddr)
			if err != nil {
				log.Printf("[NODE] STUN: failed to discover public IP: %v", err)
				continue
			}
			discoveredIP := reflexive.IP.String()
			rt.setPublicIP(discoveredIP)
			log.Printf("[NODE] STUN: discovered public IP %s (port %d)", discoveredIP, reflexive.Port)
			// Trigger an immediate signed PeerRecord re-publish so peers
			// learn the discovered public address right away instead of
			// waiting up to PeerPublisher's 5-minute periodic refresh.
			// On Fly this is a no-op because PublicDomain is already
			// set and STUN is skipped, but on non-Fly platforms the
			// re-publish is the only way reach traverses to peers.
			rt.republishWithPublicIP()
			return
		}
	}
}

// classifyNATBehaviour runs the RFC 5780 NAT-behaviour classification
// test suite via STUN and stores the result plus a derived traversal
// Strategy. The behaviour (mapping + filtering type) drives traversal-
// strategy selection — direct dial for endpoint-independent mappings,
// coordinated hole-punch for restricted, birthday-port prediction for
// symmetric — so peer_connections can consult NATStrategy() /
// PickTraversalMethod() when picking a connection path.
//
// Unlike public-IP discovery, NAT classification is needed on ALL
// platforms, including proxied clouds like Fly.io: hole-punch method
// selection depends on knowing our own NAT class regardless of how the
// platform supplies our public IP.
//
// Idempotent — "first success wins". Once natStrategy is populated a
// subsequent call short-circuits, mirroring the ORBTR agent's
// DiscoverNAT guard. Safe to invoke concurrently and off the startup
// critical path (STUN round-trips take seconds).
//
// Classification skips silently when fewer than 2 STUN servers are
// configured (RFC 5780 requires cross-server comparison for
// mapping-variance detection); the resulting zero-value behaviour means
// "detection did not complete — assume worst case."
func (rt *Runtime) classifyNATBehaviour(ctx context.Context) {
	rt.natBehaviourMu.RLock()
	haveResult := rt.natStrategy != nil
	rt.natBehaviourMu.RUnlock()
	if haveResult {
		return // idempotent — first success wins
	}

	for _, tr := range rt.listenTransports {
		if nt, ok := tr.(*noise.NoiseTransport); ok {
			stunClient := nt.GetSTUNClient()
			if stunClient == nil {
				continue
			}
			localAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", rt.cfg.VL1.UDPPort))
			behaviour, berr := stunClient.DetectNATBehaviour(ctx, localAddr)
			if berr != nil {
				log.Printf("[NODE] NAT behaviour: classification incomplete: %v (mapping=%s filtering=%s)",
					berr, behaviour.Mapping, behaviour.Filtering)
			} else {
				log.Printf("[NODE] NAT behaviour: %s (legacy=%s)", behaviour, behaviour.LegacyType())
			}
			rt.natBehaviourMu.Lock()
			rt.natBehaviour = behaviour
			// StrategyConfig takes the STUN client + optional port
			// mapper. Local/remote behaviours are passed per-call to
			// PickMethod — the Strategy itself is stateless aside from
			// its backends.
			rt.natStrategy = nat.NewStrategy(nat.StrategyConfig{
				STUN: stunClient,
			})
			rt.natBehaviourMu.Unlock()
			// STUN classification finishes asynchronously, after the
			// reach publisher's initial publish. Push the freshly
			// determined NAT class into the signed ReachRecord now so
			// peers see it on the next gossip round rather than waiting
			// for an unrelated address-change re-publish. NAT class now
			// flows through the swarm publisher's PeerRecord metadata
			// on the next publishOnce tick; encodeNATBehaviour is kept
			// here in case a future swarm-side hook needs to consume it
			// synchronously.
			_ = encodeNATBehaviour(behaviour)
			return
		}
	}
}

// NATBehaviour returns a snapshot of the node's detected RFC 5780 NAT
// behaviour. Returned value has zero-value (Mapping/FilteringUnknown)
// fields when detection hasn't completed yet or STUN is disabled.
func (rt *Runtime) NATBehaviour() nat.NATBehaviour {
	rt.natBehaviourMu.RLock()
	defer rt.natBehaviourMu.RUnlock()
	return rt.natBehaviour
}

// republishWithPublicIP triggers an immediate signed PeerRecord
// re-publish so peers see an updated public address (from STUN, from a
// bootstrap X-VL1-Observed-IP correction, or any other source that
// writes rt.publicIP) without waiting for the periodic refresh.
//
// No-op when the swarm subsystem hasn't been wired yet (early-boot path
// before PublishRPCHandlersToLAD has been called). Re-runs
// advertiseSwarmListeners first so the publisher's address set reflects
// the latest rt.publicIP / Platform.PrivateIP() / interface enumeration,
// then PublishNow forces the broadcast.
func (rt *Runtime) republishWithPublicIP() {
	if rt == nil || rt.swarm == nil || rt.swarm.Publisher == nil {
		return
	}
	rt.advertiseSwarmListeners(rt.swarm)
	rt.swarm.Publisher.PublishNow()
}

// NATStrategy returns the traversal-strategy engine initialised with
// the node's detected NAT behaviour. nil until discoverPublicIP runs
// successfully. Consumers use Strategy.PickMethod to choose between
// direct dial, coordinated hole-punch, birthday prediction, and relay
// fallback.
func (rt *Runtime) NATStrategy() *nat.Strategy {
	rt.natBehaviourMu.RLock()
	defer rt.natBehaviourMu.RUnlock()
	return rt.natStrategy
}

// PickTraversalMethod returns the recommended NAT-traversal method
// for connecting to a peer with the given remote behaviour. Returns
// nat.PunchDirect when the strategy is uninitialised (pre-STUN or STUN
// disabled) — callers should fall through to the normal direct-dial
// path in that case. On initialised strategy, the method reflects the
// local + remote NAT classifications:
//
//	PunchDirect          — one side has endpoint-independent mapping
//	PunchPortPrediction  — both sides symmetric, use birthday port picker
//	PunchUPnP            — we have a port mapper, cheaper than prediction
//	PunchRelay           — no direct path viable
//
// The returned method is advisory — the dial path is still responsible
// for executing it (direct Dial, invoking PunchRequest, requesting a
// port mapping, or routing via relay). If remoteBehaviour is zero-value
// (peer hasn't published its classification yet), assume PunchDirect
// and let TCP/WS/QUIC's listen-side reflexive-address tricks handle
// the fallback.
func (rt *Runtime) PickTraversalMethod(remote nat.NATBehaviour) nat.PunchMethod {
	rt.natBehaviourMu.RLock()
	strategy := rt.natStrategy
	local := rt.natBehaviour
	rt.natBehaviourMu.RUnlock()
	if strategy == nil {
		return nat.PunchDirect
	}
	return strategy.PickMethod(local, remote)
}

// localPunchCandidates returns the node's candidate UDP addresses for a
// coordinated hole-punch — the addresses a peer should target when it
// dials us at T0. It is the localAddrs snapshot the punchCoordinator
// calls fresh on every PunchRequest/PunchOffer build.
//
// Two sources, deduplicated, both on the VL1 UDP port:
//
//	platform public IP — FLY_PUBLIC_IP and equivalents; the dialable
//	                     address on proxied clouds where STUN is banned
//	STUN reflexive IP  — discoverPublicIP's result on platforms that
//	                     run STUN (dev, AWS, GCP, k8s)
//
// Returns an empty slice before either source has resolved (e.g. STUN
// classification still in flight) — the coordinator simply emits a
// payload with no LocalAddrs and the punch executor falls through to
// the normal dial path.
func (rt *Runtime) localPunchCandidates() []net.UDPAddr {
	port := rt.cfg.VL1.UDPPort
	if port <= 0 {
		return nil
	}
	seen := make(map[string]struct{}, 2)
	var addrs []net.UDPAddr
	add := func(raw string) {
		if raw == "" {
			return
		}
		ip := net.ParseIP(raw)
		if ip == nil {
			return
		}
		key := ip.String()
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		addrs = append(addrs, net.UDPAddr{IP: ip, Port: port})
	}
	if rt.cfg.Platform != nil {
		add(rt.cfg.Platform.PublicIP())
	}
	add(rt.getPublicIP())
	return addrs
}

// noiseTransport returns the node's Noise (VL1 UDP) transport, or nil
// when none is listening. Used by NAT-traversal code that needs
// transport-level primitives (STUN classification, PunchProbe).
func (rt *Runtime) noiseTransport() *noise.NoiseTransport {
	for _, tr := range rt.listenTransports {
		if nt, ok := tr.(*noise.NoiseTransport); ok {
			return nt
		}
	}
	return nil
}

// punchProbe is the responder half of a coordinated hole-punch wired
// into holepunchRPCHandler: it opens this node's NAT mapping toward a
// candidate address by delegating to the Noise transport's PunchProbe.
// A missing transport is reported as an error rather than a panic so a
// punch attempt before the listener is up degrades gracefully.
func (rt *Runtime) punchProbe(ctx context.Context, addr net.UDPAddr) error {
	nt := rt.noiseTransport()
	if nt == nil {
		return fmt.Errorf("holepunch: no noise transport for probe")
	}
	return nt.PunchProbe(ctx, &addr)
}

// publishPeerLatencies measures RTT to active peers and publishes
// latency records to the LAD ledger. These propagate via anchor snapshots
// so the topology API can show per-connection latency.
//
// Two sources merged: the noise-UDP transport's measured RTT is the most
// accurate signal we have for UDP peers, so it takes precedence; for any
// other transport (websocket, QUIC) the ConnectionReporter's RTT and
// transport label are used. This makes sure every active connection,
// regardless of transport, emits at least one latency record per tick
// — so a peer's presence in the live connection table always translates
// into a gossipable LAD record, even when a full-sync gossip round is
// failing (flow-control stall, session drops, etc.) and the per-round
// gossipCallback latency publisher isn't running.
type peerLatencyEntry struct {
	rtt       time.Duration
	transport string
}

func (rt *Runtime) publishPeerLatencies(ctx context.Context) {
	if rt.ledger == nil {
		return
	}

	entries := make(map[string]peerLatencyEntry, 16)
	// Primary source: noise-UDP transport — sub-ms accurate RTTs.
	for peerID, rtt := range rt.GetPeerLatencies() {
		entries[peerID] = peerLatencyEntry{rtt: rtt, transport: "noise-udp"}
	}
	// Secondary source: ConnectionReporter — covers every other transport
	// (websocket, QUIC, TCP). Noise-UDP entries are NOT overwritten.
	if rt.connMgr != nil {
		for _, ci := range NewConnectionReporter(rt.connMgr).ActiveConnections() {
			if _, ok := entries[ci.PeerNodeID]; ok {
				continue
			}
			if ci.RTT <= 0 {
				continue
			}
			entries[ci.PeerNodeID] = peerLatencyEntry{rtt: ci.RTT, transport: ci.Transport}
		}
	}

	if len(entries) == 0 {
		return
	}

	selfID := string(rt.identity.NodeID)
	published := 0
	for peerID, e := range entries {
		transport := e.transport
		if transport == "" {
			transport = "unknown"
		}
		rec := lad.LatencyRecord{
			FromNode:   selfID,
			ToNode:     peerID,
			RTTMs:      e.rtt.Milliseconds(),
			Transport:  transport,
			MeasuredAt: time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(10 * time.Minute),
		}
		body, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		if err := rt.ledger.Append(ctx, lad.Record{
			Topic:        lad.TopicLatency,
			NodeID:       selfID,
			Body:         json.RawMessage(body),
			Timestamp:    time.Now().UTC(),
			LamportClock: rt.lamportClock.Tick(),
		}); err != nil {
			continue
		}
		published++
	}
	if published > 0 {
		dbgNode.Printf("Published %d latency records", published)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Protocol-agnostic direct dial
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// DialByProtocol dials a target using the transport matching the reach address protocol.
func (rt *Runtime) DialByProtocol(ctx context.Context, proto string, nodeID string, address string) (aether.Connection, error) {
	target := aether.Target{
		NodeID:   aether.NodeID(nodeID),
		Protocol: aether.ProtocolVL1,
	}

	switch proto {
	case "udp":
		target.Address = address
		return rt.dialNoiseUDP(ctx, target)

	case "quic":
		if rt.connMgr != nil && rt.connMgr.quicTr != nil {
			target.Address = address
			return rt.connMgr.quicTr.Dial(ctx, target)
		}
		return nil, fmt.Errorf("QUIC transport not available")

	case "wss":
		if rt.connMgr != nil && rt.connMgr.wsTr != nil {
			target.Address = address // e.g., "wss://node.hstles.com/mesh/ws"
			return rt.connMgr.wsTr.Dial(ctx, target)
		}
		return nil, fmt.Errorf("WebSocket transport not available (peerMgr initializing)")

	case "tls":
		// Extract hostname from address (may be "hostname:443" or just "hostname")
		host := address
		if h, _, err := net.SplitHostPort(address); err == nil {
			host = h
		}
		result, err := rt.dialBootstrapHost(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("TLS bootstrap dial: %w", err)
		}
		// MESH-A04: wrap the bidirectional upgradedConn (reads through resp.Body,
		// writes to the TLS socket), NOT the raw TLS conn. Using rawConn dropped
		// any bytes the HTTP client's bufio reader already pulled past the 101
		// response (corrupting the first inbound frame) and leaked resp.Body,
		// which was never closed. upgradedConn.Close() closes both body and socket.
		return aether.NewConnection(rt.identity.NodeID, aether.NodeID(result.nodeID), result.conn), nil

	default:
		return nil, fmt.Errorf("unsupported protocol: %s", proto)
	}
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// LoadAdvisor implementation — provides load + grade info for RPCServer routing
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// LocalLoad returns the number of active handler goroutines on this node.
func (rt *Runtime) LocalLoad() int32 {
	if rt.rpcServer == nil {
		return 0
	}
	return rt.rpcServer.ActiveHandlers()
}

// PeerDispatchHealth returns the fraction of dispatch entries that are healthy (0.0-1.0).
func (rt *Runtime) PeerDispatchHealth() float64 {
	if rt.connMgr != nil {
		return rt.connMgr.PeerDispatchHealth()
	}
	return 0
}

// BestGradeToHandler returns the best dispatch grade to any node that handles the given handler.
func (rt *Runtime) BestGradeToHandler(handler string) Grade {
	if rt.cache == nil {
		return GradeF
	}
	role := handler
	for i, c := range handler {
		if c == '.' {
			role = handler[:i]
			break
		}
	}
	roles, err := rt.cache.Roles(context.Background(), "", ladcache.RoleQuery{Role: role})
	if err != nil || len(roles) == 0 {
		return GradeF
	}
	if rt.connMgr != nil {
		best := GradeF
		for _, rn := range roles {
			g := rt.peerGrade(rn.NodeID)
			if g > best {
				best = g
			}
		}
		return best
	}
	return GradeF
}

// publishPeerStateEvent is subscribed to ConnectionScaler events and
// writes a LAD latency record immediately on connect / upgrade / downgrade
// / disconnect, rather than waiting for the next publishPeerLatencies
// tick (up to ~30s) or for a successful gossip round (may never happen
// if gossip is stalled). This keeps the local LAD's view of the mesh
// synchronous with the actual connection table.
//
// On disconnect we emit a record with a very short ExpiresAt (30s) so
// the edge ages out of peer caches quickly rather than lingering for
// the full 10-minute TTL.
func (rt *Runtime) publishPeerStateEvent(event ConnectionEvent) {
	if rt.ledger == nil || event.PeerNodeID == "" {
		return
	}
	selfID := string(rt.identity.NodeID)
	if selfID == "" {
		return
	}

	// Connect/upgrade/downgrade events fire BEFORE the first gossip
	// exchange completes, so peer.lastRTT is often zero here. Publishing
	// a synthetic floored value masks "no measurement yet" as "1ms" in
	// the topology view; skip emission when RTT is unknown and let the
	// next publishPeerLatencies tick fill in a real measurement.
	var rtt time.Duration
	transport := event.Transport
	if rt.connMgr != nil {
		reporter := NewConnectionReporter(rt.connMgr)
		if ci, ok := reporter.ConnectionTo(event.PeerNodeID); ok {
			rtt = ci.RTT
			if transport == "" {
				transport = ci.Transport
			}
		}
	}
	if transport == "" {
		transport = "unknown"
	}

	// Short TTL on disconnect so the edge ages out quickly instead of
	// sitting for the full 10-minute window.
	expires := time.Now().UTC().Add(10 * time.Minute)
	if event.Reason == string(EventDisconnected) {
		expires = time.Now().UTC().Add(30 * time.Second)
	}

	if rtt <= 0 && event.Reason != string(EventDisconnected) {
		return
	}

	rec := lad.LatencyRecord{
		FromNode:   selfID,
		ToNode:     event.PeerNodeID,
		RTTMs:      rtt.Milliseconds(),
		Transport:  transport,
		MeasuredAt: time.Now().UTC(),
		ExpiresAt:  expires,
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := rt.ledger.Append(context.Background(), lad.Record{
		Topic:        lad.TopicLatency,
		NodeID:       selfID,
		Body:         json.RawMessage(body),
		Timestamp:    time.Now().UTC(),
		LamportClock: rt.lamportClock.Tick(),
	}); err != nil {
		return
	}
	dbgNode.Printf("Event-driven latency record: peer=%s reason=%s transport=%s rtt=%s",
		event.PeerNodeID[:12], event.Reason, transport, rtt)
}

// publishGossipLatencyWithTransport publishes a latency record with a specific transport name.
// Skips publication when RTT is non-positive — a zero/negative RTT here
// means no measurement has completed yet, not a sub-millisecond round
// trip, and emitting a synthetic 1ms hides the "no data" state.
func (rt *Runtime) publishGossipLatencyWithTransport(peerNodeID string, rtt time.Duration, transportName string) {
	if rt.ledger == nil || peerNodeID == "" || rtt <= 0 {
		return
	}
	if transportName == "" {
		transportName = "tls"
	}
	selfID := string(rt.identity.NodeID)
	rttMs := rtt.Milliseconds()
	rec := lad.LatencyRecord{
		FromNode:   selfID,
		ToNode:     peerNodeID,
		RTTMs:      rttMs,
		Transport:  transportName,
		MeasuredAt: time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(10 * time.Minute),
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_ = rt.ledger.Append(context.Background(), lad.Record{
		Topic:        lad.TopicLatency,
		NodeID:       selfID,
		Body:         json.RawMessage(body),
		Timestamp:    time.Now().UTC(),
		LamportClock: rt.lamportClock.Tick(),
	})
}

// ═══════════════════════════════════════════════════════════════════════════
// ANCHOR LEADER ELECTION
// ═══════════════════════════════════════════════════════════════════════════

// isAnchorCapable returns true if this node can serve as an anchor.
func (rt *Runtime) isAnchorCapable() bool {
	return rt.cfg.Anchor.PrivateKey != "" && rt.ledger != nil && rt.cache != nil
}

// Anchor leader election removed — all anchor-capable nodes generate
// snapshots independently. Each signs with the same ed25519 key.
// Consumers accept the highest sequence number.
