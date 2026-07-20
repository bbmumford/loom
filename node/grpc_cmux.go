/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

// ServeWithGRPC starts an HTTP server with optional gRPC multiplexing on the same port.
// If grpcServer is nil, falls back to plain HTTP. Uses cmux to split HTTP/2 gRPC
// traffic (content-type: application/grpc) from regular HTTP/1.1 and WebSocket traffic.
//
// Usage in endpoint main.go:
//
//	node.ServeWithGRPC(lis, srv, nodeRuntime.GRPCServer())
func ServeWithGRPC(lis net.Listener, httpServer *http.Server, grpcServer *grpc.Server) error {
	if grpcServer == nil {
		log.Printf("[NODE] HTTP server listening on %s", lis.Addr())
		return httpServer.Serve(lis)
	}

	m := cmux.New(lis)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	go grpcServer.Serve(grpcL)
	go httpServer.Serve(httpL)

	log.Printf("[NODE] HTTP+gRPC server listening on %s", lis.Addr())
	err := m.Serve()
	if err != nil && !strings.Contains(err.Error(), "use of closed") {
		return err
	}
	return nil
}
