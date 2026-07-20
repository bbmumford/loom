/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

// TLSConfig holds the TLS configuration for mesh networking
type TLSConfig struct {
	CertPath   string // Path to server certificate
	KeyPath    string // Path to server private key
	CAPath     string // Path to CA certificate for peer verification
	ServerName string // Expected server name for verification
}

// LoadTLSConfig loads TLS configuration from the filesystem
func LoadTLSConfig(certDir string) (*TLSConfig, error) {
	return &TLSConfig{
		CertPath:   filepath.Join(certDir, "server.crt"),
		KeyPath:    filepath.Join(certDir, "server.key"),
		CAPath:     filepath.Join(certDir, "ca.crt"),
		ServerName: "", // Will be set based on peer address
	}, nil
}

// BuildTLSConfig creates a *tls.Config for mTLS mesh networking
func (c *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("load certificate pair: %w", err)
	}

	// Load CA certificate for peer verification
	caCert, err := os.ReadFile(c.CAPath)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	// Build TLS config with mTLS (mutual authentication)
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_AES_128_GCM_SHA256,
		},
		PreferServerCipherSuites: true,
		SessionTicketsDisabled:   false,
		Renegotiation:            tls.RenegotiateNever,
	}

	return tlsConfig, nil
}

// BuildClientTLSConfig creates a client-specific TLS config for outbound Raft connections
func (c *TLSConfig) BuildClientTLSConfig(serverName string) (*tls.Config, error) {
	baseCfg, err := c.BuildTLSConfig()
	if err != nil {
		return nil, err
	}

	// Clone and modify for client use
	clientCfg := baseCfg.Clone()
	clientCfg.ServerName = serverName
	clientCfg.InsecureSkipVerify = false // Always verify server certificates

	return clientCfg, nil
}

// ValidateTLSCertificates checks that all required certificate files exist and are readable
func (c *TLSConfig) ValidateTLSCertificates() error {
	files := map[string]string{
		"server certificate": c.CertPath,
		"server key":         c.KeyPath,
		"CA certificate":     c.CAPath,
	}

	for name, path := range files {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%s not found at %s", name, path)
			}
			return fmt.Errorf("cannot access %s at %s: %w", name, path, err)
		}
	}

	// Try to load and parse certificates to validate format
	if _, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath); err != nil {
		return fmt.Errorf("invalid certificate/key pair: %w", err)
	}

	caCert, err := os.ReadFile(c.CAPath)
	if err != nil {
		return fmt.Errorf("read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to parse CA certificate")
	}

	return nil
}

// GetTLSConfigFromEnv creates TLSConfig from environment variables
// Falls back to ./certificates directory if not specified
func GetTLSConfigFromEnv() (*TLSConfig, error) {
	certDir := os.Getenv("MESH_CERT_DIR")
	if certDir == "" {
		certDir = "./certificates/mesh"
	}

	tlsCfg, err := LoadTLSConfig(certDir)
	if err != nil {
		return nil, err
	}

	// Allow environment variable overrides for individual files
	if certPath := os.Getenv("MESH_CERT_PATH"); certPath != "" {
		tlsCfg.CertPath = certPath
	}
	if keyPath := os.Getenv("MESH_KEY_PATH"); keyPath != "" {
		tlsCfg.KeyPath = keyPath
	}
	if caPath := os.Getenv("MESH_CA_PATH"); caPath != "" {
		tlsCfg.CAPath = caPath
	}

	return tlsCfg, nil
}

// raftTLSStreamLayer implements raft.StreamLayer with TLS support
type raftTLSStreamLayer struct {
	listener net.Listener
	tlsCfg   *tls.Config
}

// NewRaftTLSStreamLayer creates a TLS-enabled stream layer for Raft transport
func NewRaftTLSStreamLayer(bindAddr string, tlsCfg *tls.Config) (*raftTLSStreamLayer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("create TCP listener: %w", err)
	}

	tlsListener := tls.NewListener(listener, tlsCfg)

	return &raftTLSStreamLayer{
		listener: tlsListener,
		tlsCfg:   tlsCfg,
	}, nil
}

func (s *raftTLSStreamLayer) Accept() (net.Conn, error) {
	return s.listener.Accept()
}

func (s *raftTLSStreamLayer) Close() error {
	return s.listener.Close()
}

func (s *raftTLSStreamLayer) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *raftTLSStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	// Parse server name from address for TLS verification
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		host = string(address)
	}

	tlsConfigCopy := s.tlsCfg.Clone()
	tlsConfigCopy.ServerName = host

	return tls.DialWithDialer(dialer, "tcp", string(address), tlsConfigCopy)
}
