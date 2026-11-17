// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/config/security.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"slices"

	"github.com/pkg/errors"
)

// Security is the security section of the config.
type Security struct {
	ClusterSSLCA    string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert  string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey   string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
	// KVServerVerifyCNMode is off (including the empty default) or enforce.
	// This policy applies only to outbound KV RPC connections.
	KVServerVerifyCNMode string   `toml:"kv-server-verify-cn-mode" json:"kv-server-verify-cn-mode"`
	KVServerVerifyCN     []string `toml:"kv-server-verify-cn" json:"kv-server-verify-cn"`
}

// ValidateKVServerCN validates the independent outbound server identity policy.
func (s *Security) ValidateKVServerCN() error {
	switch s.KVServerVerifyCNMode {
	case "", "off":
		return nil
	case "enforce":
		if s.ClusterSSLCA == "" || len(s.KVServerVerifyCN) == 0 {
			return errors.New("kv-server-verify-cn-mode=enforce requires cluster-ssl-ca and a nonempty kv-server-verify-cn")
		}
		for _, cn := range s.KVServerVerifyCN {
			if cn == "" {
				return errors.New("kv-server-verify-cn must not contain an empty CN")
			}
		}
		return nil
	default:
		return errors.New("kv-server-verify-cn-mode must be off or enforce")
	}
}

// ToKVServerTLSConfig adds an exact server leaf CN check after standard TLS
// chain and SAN verification. The policy is a snapshot owned by one pool
// generation, including during preflight and subsequent reconnections.
func (s *Security) ToKVServerTLSConfig() (*tls.Config, error) {
	if err := s.ValidateKVServerCN(); err != nil {
		return nil, err
	}
	cfg, err := s.ToTLSConfig()
	if err != nil || cfg == nil || s.KVServerVerifyCNMode != "enforce" {
		return cfg, err
	}
	allowed := slices.Clone(s.KVServerVerifyCN)
	cfg.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return errors.New("KV server certificate has no verified chain")
		}
		cn := state.PeerCertificates[0].Subject.CommonName
		if !slices.Contains(allowed, cn) {
			return errors.Errorf("KV server certificate CN %q is not in kv-server-verify-cn", cn)
		}
		return nil
	}
	return cfg, nil
}

// NewSecurity creates a Security.
func NewSecurity(sslCA, sslCert, sslKey string, verityCN []string) Security {
	return Security{
		ClusterSSLCA:    sslCA,
		ClusterSSLCert:  sslCert,
		ClusterSSLKey:   sslKey,
		ClusterVerifyCN: verityCN,
	}
}

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error) {
	if len(s.ClusterSSLCA) != 0 {
		certPool := x509.NewCertPool()
		// Create a certificate pool from the certificate authority
		var ca []byte
		ca, err = os.ReadFile(s.ClusterSSLCA)
		if err != nil {
			err = errors.Errorf("could not read ca certificate: %s", err)
			return
		}
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			err = errors.New("failed to append ca certs")
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:   certPool,
			ClientCAs: certPool,
		}

		if len(s.ClusterSSLCert) != 0 && len(s.ClusterSSLKey) != 0 {
			getCert := func() (*tls.Certificate, error) {
				// Load the client certificates from disk
				cert, err := tls.LoadX509KeyPair(s.ClusterSSLCert, s.ClusterSSLKey)
				if err != nil {
					return nil, errors.Errorf("could not load client key pair: %s", err)
				}
				return &cert, nil
			}
			// pre-test cert's loading.
			if _, err = getCert(); err != nil {
				return
			}
			tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
			tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
		}
	}
	return
}
