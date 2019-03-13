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
)
