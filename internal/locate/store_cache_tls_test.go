// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package locate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type healthTestPKI struct {
	security config.Security
	server   *tls.Config
}

func newHealthTestPKI(t *testing.T) healthTestPKI {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-health-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "test-health"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	write := func(name, kind string, der []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600))
		return path
	}
	security := config.NewSecurity(write("ca.pem", "CERTIFICATE", caDER), write("cert.pem", "CERTIFICATE", leafDER), write("key.pem", "PRIVATE KEY", keyDER), nil)
	server, err := security.ToTLSConfig()
	require.NoError(t, err)
	server.MinVersion = tls.VersionTLS12
	server.ClientAuth = tls.RequireAndVerifyClientCert
	return healthTestPKI{security: security, server: server}
}

func startHealthTestServer(t *testing.T, pki *healthTestPKI, interceptor grpc.UnaryServerInterceptor) (string, *health.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var options []grpc.ServerOption
	if pki != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(pki.server)))
	}
	if interceptor != nil {
		options = append(options, grpc.UnaryInterceptor(interceptor))
	}
	server := grpc.NewServer(options...)
	service := health.NewServer()
	service.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String(), service
}

func setHealthTestGlobalSecurity(t *testing.T, security config.Security) {
	t.Helper()
	previous := config.GetGlobalConfig()
	config.UpdateGlobal(func(c *config.Config) { c.Security = security })
	t.Cleanup(func() { config.StoreGlobalConfig(previous) })
}

func TestStoreLivenessConnectorMTLS(t *testing.T) {
	setHealthTestGlobalSecurity(t, config.Security{})
	a, b := newHealthTestPKI(t), newHealthTestPKI(t)
	addrA, _ := startHealthTestServer(t, &a, nil)
	addrB, _ := startHealthTestServer(t, &b, nil)
	require.Equal(t, unreachable, invokeKVStatusAPI(addrA, 250*time.Millisecond, nil), "legacy empty globals cannot authenticate")
	require.Equal(t, reachable, invokeKVStatusAPI(addrA, time.Second, &a.security))
	require.Equal(t, reachable, invokeKVStatusAPI(addrB, time.Second, &b.security))
	require.Equal(t, unreachable, invokeKVStatusAPI(addrA, 250*time.Millisecond, &b.security), "wrong CA must never fall back")
	withoutClient := config.NewSecurity(a.security.ClusterSSLCA, "", "", nil)
	require.Equal(t, unreachable, invokeKVStatusAPI(addrA, 250*time.Millisecond, &withoutClient), "mTLS requires client identity")
	require.Equal(t, config.Security{}, config.GetGlobalConfig().Security)
}

func TestStoreLivenessExplicitPlaintext(t *testing.T) {
	pki := newHealthTestPKI(t)
	setHealthTestGlobalSecurity(t, pki.security)
	addr, _ := startHealthTestServer(t, nil, nil)
	empty := config.Security{}
	require.Equal(t, reachable, invokeKVStatusAPI(addr, time.Second, &empty))
	require.Equal(t, unreachable, invokeKVStatusAPI(addr, 250*time.Millisecond, nil), "omitted policy retains global TLS")
}

func TestStoreLivenessDoesNotShareAnotherConnectorsResult(t *testing.T) {
	a, b := newHealthTestPKI(t), newHealthTestPKI(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	t.Cleanup(resume)
	addr, _ := startHealthTestServer(t, &a, func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return handler(ctx, req)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	good, bad := &storeCacheImpl{}, &storeCacheImpl{}
	good.setStoreLivenessSecurity(a.security)
	bad.setStoreLivenessSecurity(b.security)
	store := newStore(1, addr, "", "", tikvrpc.TiKV, resolved, nil)
	result := make(chan livenessState, 1)
	go func() { result <- requestLiveness(context.Background(), store, good) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("authenticated health request did not enter server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.Equal(t, unreachable, requestLiveness(ctx, store, bad), "untrusted connector must not inherit an authenticated in-flight probe")
	good.setStoreLivenessSecurity(b.security)
	require.Equal(t, unreachable, requestLiveness(ctx, store, good), "new security must not inherit the previous policy's in-flight probe")
	resume()
	require.Equal(t, reachable, <-result)
}

func TestStoreLivenessMTLSBackgroundRecovery(t *testing.T) {
	setHealthTestGlobalSecurity(t, config.Security{})
	pki := newHealthTestPKI(t)
	addr, service := startHealthTestServer(t, &pki, nil)
	cache := &storeCacheImpl{}
	regionCache := &RegionCache{stores: cache}
	regionCache.SetStoreLivenessSecurity(pki.security)
	store := newStore(1, addr, "", "", tikvrpc.TiKV, resolved, nil)
	scheduler := newBackgroundRunner(context.Background())
	t.Cleanup(func() { scheduler.shutdown(true) })
	service.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	require.Equal(t, unreachable, store.requestLivenessAndStartHealthCheckLoopIfNeeded(retry.NewBackofferWithVars(context.Background(), 1000, nil), scheduler, cache))
	require.Equal(t, unreachable, store.getLivenessState())
	service.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	require.Eventually(t, func() bool { return store.getLivenessState() == reachable }, 3*time.Second, 10*time.Millisecond)
}
