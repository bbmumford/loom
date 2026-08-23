/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package meshstatus

import (
	"context"
	"net/http"
	"time"

	"github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/ports"
)

// NodeRuntime is the interface that node.Runtime satisfies.
// Defined here to avoid importing the node package (which would be circular).
type NodeRuntime interface {
	NodeID() aether.NodeID
	MeshMetrics() map[string]interface{}
	Directory() *ladcache.DirectoryCache
	Config() interface{ GetServiceName() string }
}

// runtimeAdapter wraps a node.Runtime to satisfy RuntimeProvider.
type runtimeAdapter struct {
	nodeID      aether.NodeID
	vl1Metrics  func() map[string]interface{}
	directory   DirectoryProvider
	serviceName string
}

func (a *runtimeAdapter) NodeID() aether.NodeID               { return a.nodeID }
func (a *runtimeAdapter) MeshMetrics() map[string]interface{} { return a.vl1Metrics() }
func (a *runtimeAdapter) Directory() DirectoryProvider        { return a.directory }
func (a *runtimeAdapter) ServiceName() string                 { return a.serviceName }

// directoryCacheAdapter wraps *ladcache.DirectoryCache to satisfy DirectoryProvider.
type directoryCacheAdapter struct {
	cache *ladcache.DirectoryCache
}

func (d *directoryCacheAdapter) Members(ctx context.Context, tenant string) ([]lad.MemberRecord, error) {
	return d.cache.Members(ctx, tenant)
}

func (d *directoryCacheAdapter) Roles(ctx context.Context, tenant string, query ladcache.RoleQuery) ([]lad.RoleRecord, error) {
	return d.cache.Roles(ctx, tenant, query)
}

func (d *directoryCacheAdapter) Reach(ctx context.Context, tenant string, query ladcache.ReachQuery) ([]lad.ReachRecord, error) {
	return d.cache.Reach(ctx, tenant, query)
}

// NewHandlerFromRuntime creates a /mesh/status handler from runtime components.
// This is the convenience constructor used by each endpoint's main.go.
// It avoids circular imports by accepting the individual components rather than
// the full node.Runtime struct.
func NewHandlerFromRuntime(
	nodeID aether.NodeID,
	vl1MetricsFn func() map[string]interface{},
	directory *ladcache.DirectoryCache,
	serviceName string,
	platform ports.PlatformInfo,
	startTime time.Time,
) http.HandlerFunc {
	if platform == nil {
		platform = ports.DevPlatform()
	}
	var dirProvider DirectoryProvider
	if directory != nil {
		dirProvider = &directoryCacheAdapter{cache: directory}
	}
	adapter := &runtimeAdapter{
		nodeID:      nodeID,
		vl1Metrics:  vl1MetricsFn,
		directory:   dirProvider,
		serviceName: serviceName,
	}
	return NewHandler(adapter, platform, startTime)
}
