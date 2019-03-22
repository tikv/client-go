// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"
)

// Security is SSL configuration.
type Security struct {
	SSLCA   string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey  string `toml:"ssl-key" json:"ssl-key"`
}

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTLSConfig() (*tls.Config, error) {
	var tlsConfig *tls.Config
	if len(s.SSLCA) != 0 {
		var certificates = make([]tls.Certificate, 0)
		if len(s.SSLCert) != 0 && len(s.SSLKey) != 0 {
			// Load the client certificates from disk
			certificate, err := tls.LoadX509KeyPair(s.SSLCert, s.SSLKey)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(s.SSLCA)
		if err != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", err)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConfig = &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		}
	}

	return tlsConfig, nil
}

// EnableOpenTracing is the flag to enable open tracing.
var EnableOpenTracing = false

var (
	// OverloadThreshold is a threshold of TiKV load.
	// If TiKV load is greater than this, TiDB will wait for a while to avoid little batch.
	OverloadThreshold uint = 200
	// BatchWaitSize is the max wait size for batch.
	BatchWaitSize uint = 8
	// MaxBatchSize is the max batch size when calling batch commands API.
	MaxBatchSize uint = 128
	// MaxBatchWaitTime in nanosecond is the max wait time for batch.
	MaxBatchWaitTime time.Duration
)

// Those limits are enforced to make sure the transaction can be well handled by TiKV.
var (
	// TxnEntrySizeLimit is limit of single entry size (len(key) + len(value)).
	TxnEntrySizeLimit = 6 * 1024 * 1024
	// TxnEntryCountLimit is a limit of the number of entries in the MemBuffer.
	TxnEntryCountLimit uint64 = 300 * 1000
	// TxnTotalSizeLimit is limit of the sum of all entry size.
	TxnTotalSizeLimit = 100 * 1024 * 1024
	// MaxTxnTimeUse is the max time a transaction can run.
	MaxTxnTimeUse = 590
)

// Local latches for transactions. Enable it when
// there are lots of conflicts between transactions.
var (
	EnableTxnLocalLatch        = false
	TxnLocalLatchCapacity uint = 2048000
)

// RegionCache configurations.
var (
	RegionCacheBTreeDegree = 32
	RegionCacheTTL         = time.Minute * 10
)

// RawKV configurations.
var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// rawBatchPutSize is the maximum size limit for rawkv each batch put request.
	RawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawkv each batch get/delete request.
	RawBatchPairCount = 512
)

// RPC configurations.
var (
	// MaxConnectionCount is the max gRPC connections that will be established with
	// each tikv-server.
	MaxConnectionCount uint = 16

	// GrpcKeepAliveTime is the duration of time after which if the client doesn't see
	// any activity it pings the server to see if the transport is still alive.
	GrpcKeepAliveTime = time.Duration(10) * time.Second

	// GrpcKeepAliveTimeout is the duration of time for which the client waits after having
	// pinged for keepalive check and if no activity is seen even after that the connection
	// is closed.
	GrpcKeepAliveTimeout = time.Duration(3) * time.Second

	// MaxSendMsgSize set max gRPC request message size sent to server. If any request message size is larger than
	// current value, an error will be reported from gRPC.
	MaxSendMsgSize = 1<<31 - 1

	// MaxCallMsgSize set max gRPC receive message size received from server. If any message size is larger than
	// current value, an error will be reported from gRPC.
	MaxCallMsgSize = 1<<31 - 1

	DialTimeout               = 5 * time.Second
	ReadTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute

	GrpcInitialWindowSize     = 1 << 30
	GrpcInitialConnWindowSize = 1 << 30
)

// KV configurations.
var (
	// DefaultTxnMembufCap is the default transaction membuf capability.
	DefaultTxnMembufCap = 4 * 1024
)

// Latch configurations.
var (
	LatchExpireDuration = 2 * time.Minute
	LatchCheckInterval  = 1 * time.Minute
	LatchCheckCounter   = 50000
	LatchListCount      = 5
	LatchLockChanSize   = 100
)

// Oracle configurations.
var (
	TsoSlowThreshold = 30 * time.Millisecond
	// update oracle's lastTS every 2000ms.
	OracleUpdateInterval = 2000
)

// Txn configurations.
var (
	// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
	// Key+Value size below 16KB.
	TxnCommitBatchSize = 16 * 1024

	// By default, locks after 3000ms is considered unusual (the client created the
	// lock might be dead). Other client may cleanup this kind of lock.
	// For locks created recently, we will do backoff and retry.
	TxnDefaultLockTTL uint64 = 3000
	// TODO: Consider if it's appropriate.
	TxnMaxLockTTL uint64 = 120000
	// ttl = ttlFactor * sqrt(writeSizeInMiB)
	TxnTTLFactor = 6000
	// TxnResolvedCacheSize is max number of cached txn status.
	TxnResolvedCacheSize = 2048

	// SafePoint.
	// This is almost the same as 'tikv_gc_safe_point' in the table 'mysql.tidb',
	// save this to pd instead of tikv, because we can't use interface of table
	// if the safepoint on tidb is expired.
	GcSavedSafePoint = "/tidb/store/gcworker/saved_safe_point"

	GcSafePointCacheInterval       = time.Second * 100
	GcCPUTimeInaccuracyBound       = time.Second
	GcSafePointUpdateInterval      = time.Second * 10
	GcSafePointQuickRepeatInterval = time.Second

	TxnScanBatchSize = 256
	TxnBatchGetSize  = 5120
)
