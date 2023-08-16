// Copyright 2023 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/execdetails.go
//

// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

const (
	// pd request retry time when connection fail.
	pdRequestRetryTime = 10

	minResolvedTSPrefix = "pd/api/v1/min-resolved-ts"
)

// PDHTTPClient is an HTTP client of pd.
type PDHTTPClient struct {
	addrs []string
	cli   *http.Client
}

func NewPDHTTPClient(
	tlsConf *tls.Config,
	pdAddrs []string,
) *PDHTTPClient {
	for i, addr := range pdAddrs {
		if !strings.HasPrefix(addr, "http") {
			if tlsConf != nil {
				addr = "https://" + addr
			} else {
				addr = "http://" + addr
			}
			pdAddrs[i] = addr
		}
	}

	return &PDHTTPClient{
		addrs: pdAddrs,
		cli:   httpClient(tlsConf),
	}
}

// GetMinResolvedTSByStoresIDs get min-resolved-ts from pd by stores ids.
func (p *PDHTTPClient) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []string) (uint64, map[uint64]uint64, error) {
	var err error
	for _, addr := range p.addrs {
		// scope is an optional parameter, it can be `cluster` or specified store IDs.
		// - When no scope is given, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be nil.
		// - When scope is `cluster`, cluster-level's min_resolved_ts will be returned and storesMinResolvedTS will be filled.
		// - When scope given a list of stores, min_resolved_ts will be provided for each store
		//      and the scope-specific min_resolved_ts will be returned.
		query := minResolvedTSPrefix
		if len(storeIDs) != 0 {
			query = fmt.Sprintf("%s?scope=%s", query, strings.Join(storeIDs, ","))
		}
		v, e := pdRequest(ctx, addr, query, p.cli, http.MethodGet, nil)
		if e != nil {
			logutil.BgLogger().Debug("failed to get min resolved ts", zap.String("addr", addr), zap.Error(e))
			err = e
			continue
		}
		logutil.BgLogger().Debug("min resolved ts", zap.String("resp", string(v)))
		d := struct {
			MinResolvedTS       uint64            `json:"min_resolved_ts"`
			IsRealTime          bool              `json:"is_real_time,omitempty"`
			StoresMinResolvedTS map[uint64]uint64 `json:"stores_min_resolved_ts"`
		}{}
		err = json.Unmarshal(v, &d)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if !d.IsRealTime {
			message := fmt.Errorf("min resolved ts not enabled, addr: %s", addr)
			logutil.BgLogger().Debug(message.Error())
			return 0, nil, errors.Trace(message)
		}
		if val, e := EvalFailpoint("InjectMinResolvedTS"); e == nil {
			// Need to make sure successfully get from real pd.
			if d.StoresMinResolvedTS != nil {
				for storeID, v := range d.StoresMinResolvedTS {
					if v != 0 {
						// Should be val.(uint64) but failpoint doesn't support that.
						if tmp, ok := val.(int); ok {
							d.StoresMinResolvedTS[storeID] = uint64(tmp)
							logutil.BgLogger().Info("inject min resolved ts", zap.Uint64("storeID", storeID), zap.Uint64("ts", uint64(tmp)))
						}
					}
				}
			} else if tmp, ok := val.(int); ok {
				// Should be val.(uint64) but failpoint doesn't support that.
				// ci's store id is 1, we can change it if we have more stores.
				// but for pool ci it's no need to do that :(
				d.MinResolvedTS = uint64(tmp)
				logutil.BgLogger().Info("inject min resolved ts", zap.Uint64("ts", uint64(tmp)))
			}

		}

		return d.MinResolvedTS, d.StoresMinResolvedTS, nil
	}

	return 0, nil, errors.Trace(err)
}

// pdRequest is a func to send an HTTP to pd and return the result bytes.
func pdRequest(
	ctx context.Context,
	addr string, prefix string,
	cli *http.Client, method string, body io.Reader) ([]byte, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqURL := fmt.Sprintf("%s/%s", u, prefix)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var resp *http.Response
	count := 0
	for {
		resp, err = cli.Do(req)
		count++

		if _, e := EvalFailpoint("InjectClosed"); e == nil {
			resp = nil
			err = &url.Error{
				Op:  "read",
				Err: os.NewSyscallError("connect", syscall.ECONNREFUSED),
			}
			return nil, errors.Trace(err)
		}

		if count > pdRequestRetryTime || (resp != nil && resp.StatusCode < 500) ||
			err != nil {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(pdRequestRetryInterval())
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		res, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("[%d] %s %s", resp.StatusCode, res, reqURL)
	}

	r, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return r, err
}

func pdRequestRetryInterval() time.Duration {
	if _, e := EvalFailpoint("FastRetry"); e == nil {
		return 0
	}
	return time.Second
}

// httpClient returns an HTTP(s) client.
func httpClient(tlsConf *tls.Config) *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	return cli
}

func (p *PDHTTPClient) Close() {
	p.cli.CloseIdleConnections()
	logutil.BgLogger().Info("closed pd http client")
}
