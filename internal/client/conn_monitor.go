// Copyright 2025 TiKV Authors
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

package client

import (
	"sync"
	"time"

	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type monitoredConn struct {
	*grpc.ClientConn
	Name string
}

func (c *monitoredConn) Close() error {
	if c.ClientConn != nil {
		err := c.ClientConn.Close()
		logutil.BgLogger().Debug("close gRPC connection", zap.String("target", c.Name), zap.Error(err))
		return err
	}
	return nil
}

type connMonitor struct {
	m        sync.Map
	loopOnce sync.Once
	stopOnce sync.Once
	stop     chan struct{}
}

func (c *connMonitor) AddConn(conn *monitoredConn) {
	c.m.Store(conn.Name, conn)
}

func (c *connMonitor) RemoveConn(conn *monitoredConn) {
	c.m.Delete(conn.Name)
	for state := connectivity.Idle; state <= connectivity.Shutdown; state++ {
		metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), state.String()).Set(0)
	}
}

func (c *connMonitor) Start() {
	c.loopOnce.Do(
		func() {
			c.stop = make(chan struct{})
			go c.start()
		},
	)
}

func (c *connMonitor) Stop() {
	c.stopOnce.Do(
		func() {
			if c.stop != nil {
				close(c.stop)
			}
		},
	)
}

func (c *connMonitor) start() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.m.Range(func(_, value interface{}) bool {
				conn := value.(*monitoredConn)
				nowState := conn.GetState()
				for state := connectivity.Idle; state <= connectivity.Shutdown; state++ {
					if state == nowState {
						metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), nowState.String()).Set(1)
					} else {
						metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), state.String()).Set(0)
					}
				}
				return true
			})
		case <-c.stop:
			return
		}
	}
}
