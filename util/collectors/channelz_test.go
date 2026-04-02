// Copyright 2026 TiKV Authors
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

package collectors

import (
	"context"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	channelzpb "google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestChannelzCollectorCollect(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref: &channelzpb.ChannelRef{ChannelId: 1},
						Data: &channelzpb.ChannelData{
							Target:                   "cluster-a",
							State:                    &channelzpb.ChannelConnectivityState{State: channelzpb.ChannelConnectivityState_READY},
							CallsStarted:             12,
							CallsSucceeded:           10,
							CallsFailed:              2,
							LastCallStartedTimestamp: timestamppb.New(base.Add(-20 * time.Second)),
							Trace: &channelzpb.ChannelTrace{
								NumEventsLogged:   4,
								CreationTimestamp: timestamppb.New(base.Add(-5 * time.Minute)),
								Events: []*channelzpb.ChannelTraceEvent{
									{Severity: channelzpb.ChannelTraceEvent_CT_INFO},
									{Severity: channelzpb.ChannelTraceEvent_CT_WARNING},
									{Severity: channelzpb.ChannelTraceEvent_CT_ERROR},
								},
							},
						},
						SubchannelRef: []*channelzpb.SubchannelRef{{SubchannelId: 2}},
						SocketRef:     []*channelzpb.SocketRef{{SocketId: 10}},
					},
				},
				End: false,
			},
			2: {
				Channel: []*channelzpb.Channel{
					{
						Ref: &channelzpb.ChannelRef{ChannelId: 3},
						Data: &channelzpb.ChannelData{
							Target:         "cluster-b",
							State:          &channelzpb.ChannelConnectivityState{State: channelzpb.ChannelConnectivityState_IDLE},
							CallsStarted:   2,
							CallsSucceeded: 2,
						},
					},
				},
				End: true,
			},
		},
		subchannels: map[int64]*channelzpb.Subchannel{
			2: {
				Ref: &channelzpb.SubchannelRef{SubchannelId: 2},
				Data: &channelzpb.ChannelData{
					Target:         "backend-a",
					State:          &channelzpb.ChannelConnectivityState{State: channelzpb.ChannelConnectivityState_CONNECTING},
					CallsStarted:   3,
					CallsSucceeded: 2,
					CallsFailed:    1,
				},
				SocketRef: []*channelzpb.SocketRef{{SocketId: 10}, {SocketId: 11}},
			},
		},
		sockets: map[int64]*channelzpb.Socket{
			10: {
				Ref:    &channelzpb.SocketRef{SocketId: 10},
				Local:  tcpAddress(net.ParseIP("127.0.0.1"), 8080),
				Remote: tcpAddress(net.ParseIP("2001:db8::1"), 443),
				Data: &channelzpb.SocketData{
					StreamsStarted:                   5,
					StreamsSucceeded:                 3,
					StreamsFailed:                    1,
					MessagesSent:                     9,
					MessagesReceived:                 8,
					KeepAlivesSent:                   4,
					LastLocalStreamCreatedTimestamp:  timestamppb.New(base.Add(-11 * time.Second)),
					LastRemoteStreamCreatedTimestamp: timestamppb.New(base.Add(-13 * time.Second)),
					LastMessageSentTimestamp:         timestamppb.New(base.Add(-5 * time.Second)),
					LastMessageReceivedTimestamp:     timestamppb.New(base.Add(-7 * time.Second)),
					LocalFlowControlWindow:           wrapperspb.Int64(128),
					RemoteFlowControlWindow:          wrapperspb.Int64(256),
				},
			},
			11: {
				Ref:    &channelzpb.SocketRef{SocketId: 11},
				Local:  udsAddress("/tmp/channelz.sock"),
				Remote: otherAddress("passthrough:///peer"),
				Data: &channelzpb.SocketData{
					StreamsStarted:               2,
					StreamsSucceeded:             1,
					StreamsFailed:                1,
					LastMessageSentTimestamp:     timestamppb.New(base.Add(-9 * time.Second)),
					LastMessageReceivedTimestamp: timestamppb.New(base.Add(-3 * time.Second)),
				},
			},
		},
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)

	requireMetricValue(t, families["grpc_channelz_channel_calls_total"], map[string]string{
		"kind":   "channel",
		"id":     "1",
		"target": "cluster-a",
		"type":   "started",
	}, 12)
	requireMetricValue(t, families["grpc_channelz_channel_calls_total"], map[string]string{
		"kind":   "subchannel",
		"id":     "2",
		"target": "backend-a",
		"type":   "failed",
	}, 1)
	require.Nil(t, families["grpc_channelz_channel_state"])
	require.Nil(t, families["grpc_channelz_channel_trace_event_count"])

	socket10Labels := map[string]string{
		"id":     "10",
		"local":  "127.0.0.1:8080",
		"remote": "[2001:db8::1]:443",
	}
	requireMetricValue(t, families["grpc_channelz_socket_streams_total"], withLabels(socket10Labels, "type", "started"), 5)
	requireMetricValue(t, families["grpc_channelz_socket_messages_total"], withLabels(socket10Labels, "direction", "sent"), 9)
	requireMetricValue(t, families["grpc_channelz_socket_keepalives_total"], socket10Labels, 4)
	requireMetricValue(t, families["grpc_channelz_socket_flow_control_window_bytes"], withLabels(socket10Labels, "side", "remote"), 256)
	require.Equal(t, 1, countMetrics(t, families["grpc_channelz_socket_streams_total"], withLabels(socket10Labels, "type", "started")))

	socket11Labels := map[string]string{
		"id":     "11",
		"local":  "/tmp/channelz.sock",
		"remote": "passthrough:///peer",
	}
	requireMetricValue(t, families["grpc_channelz_socket_streams_total"], withLabels(socket11Labels, "type", "failed"), 1)
	require.Nil(t, families["grpc_channelz_socket_active_streams"])
	require.Nil(t, families["grpc_channelz_socket_failed_stream_ratio"])
	require.Nil(t, families["grpc_channelz_socket_message_receive_lag_seconds"])
}

func TestChannelzCollectorChannelOptions(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref: &channelzpb.ChannelRef{ChannelId: 1},
						Data: &channelzpb.ChannelData{
							Target:                   "cluster-a",
							State:                    &channelzpb.ChannelConnectivityState{State: channelzpb.ChannelConnectivityState_READY},
							CallsStarted:             1,
							LastCallStartedTimestamp: timestamppb.New(base.Add(-20 * time.Second)),
							Trace: &channelzpb.ChannelTrace{
								NumEventsLogged:   2,
								CreationTimestamp: timestamppb.New(base.Add(-5 * time.Minute)),
								Events: []*channelzpb.ChannelTraceEvent{
									{Severity: channelzpb.ChannelTraceEvent_CT_WARNING},
								},
							},
						},
					},
				},
				End: true,
			},
		},
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{
		IncludeChannelState: true,
		IncludeChannelTrace: true,
	})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)

	requireMetricValue(t, families["grpc_channelz_channel_state"], map[string]string{
		"kind":   "channel",
		"id":     "1",
		"target": "cluster-a",
		"state":  "ready",
	}, 1)
	requireMetricValue(t, families["grpc_channelz_channel_state"], map[string]string{
		"kind":   "channel",
		"id":     "1",
		"target": "cluster-a",
		"state":  "idle",
	}, 0)
	requireMetricValue(t, families["grpc_channelz_channel_trace_events_logged_total"], map[string]string{
		"kind":   "channel",
		"id":     "1",
		"target": "cluster-a",
	}, 2)
	requireMetricValue(t, families["grpc_channelz_channel_trace_event_count"], map[string]string{
		"kind":     "channel",
		"id":       "1",
		"target":   "cluster-a",
		"severity": "warning",
	}, 1)
}

func TestChannelzCollectorFilter(t *testing.T) {
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref:           &channelzpb.ChannelRef{ChannelId: 1},
						Data:          &channelzpb.ChannelData{Target: "root"},
						ChannelRef:    []*channelzpb.ChannelRef{{ChannelId: 5}},
						SubchannelRef: []*channelzpb.SubchannelRef{{SubchannelId: 2}},
					},
					{
						Ref:       &channelzpb.ChannelRef{ChannelId: 4},
						Data:      &channelzpb.ChannelData{Target: "blocked"},
						SocketRef: []*channelzpb.SocketRef{{SocketId: 11}},
					},
				},
				End: true,
			},
		},
		channels: map[int64]*channelzpb.Channel{
			5: {
				Ref:       &channelzpb.ChannelRef{ChannelId: 5},
				Data:      &channelzpb.ChannelData{Target: "child"},
				SocketRef: []*channelzpb.SocketRef{{SocketId: 13}},
			},
		},
		subchannels: map[int64]*channelzpb.Subchannel{
			2: {
				Ref:       &channelzpb.SubchannelRef{SubchannelId: 2},
				Data:      &channelzpb.ChannelData{Target: "sub"},
				SocketRef: []*channelzpb.SocketRef{{SocketId: 10}},
			},
		},
		sockets: map[int64]*channelzpb.Socket{
			10: {Ref: &channelzpb.SocketRef{SocketId: 10}, Data: &channelzpb.SocketData{StreamsStarted: 1}},
			11: {Ref: &channelzpb.SocketRef{SocketId: 11}, Data: &channelzpb.SocketData{StreamsStarted: 1}},
			13: {Ref: &channelzpb.SocketRef{SocketId: 13}, Data: &channelzpb.SocketData{StreamsStarted: 2}},
		},
	}

	filter := func(node any) (bool, bool) {
		switch node := node.(type) {
		case *channelzpb.Channel:
			switch node.GetRef().GetChannelId() {
			case 1:
				return false, true
			case 4:
				return false, false
			}
		case *channelzpb.Subchannel:
			if node.GetRef().GetSubchannelId() == 2 {
				return true, false
			}
		}
		return true, true
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{Filter: filter})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)

	require.Equal(t, 0, countMetrics(t, families["grpc_channelz_channel_calls_total"], map[string]string{"id": "1"}))
	require.Equal(t, 1, countMetrics(t, families["grpc_channelz_channel_calls_total"], map[string]string{"kind": "subchannel", "id": "2", "target": "sub", "type": "started"}))
	require.Equal(t, 0, countMetrics(t, families["grpc_channelz_socket_streams_total"], map[string]string{"id": "10"}))
	require.Equal(t, 1, countMetrics(t, families["grpc_channelz_channel_calls_total"], map[string]string{"kind": "channel", "id": "5", "target": "child", "type": "started"}))
	require.Equal(t, 1, countMetrics(t, families["grpc_channelz_socket_streams_total"], map[string]string{"id": "13", "type": "started"}))
	require.Equal(t, 0, countMetrics(t, families["grpc_channelz_channel_calls_total"], map[string]string{"id": "4"}))
	require.Equal(t, 0, countMetrics(t, families["grpc_channelz_socket_streams_total"], map[string]string{"id": "11"}))
}

func TestChannelzCollectorSocketLabelToggles(t *testing.T) {
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref:       &channelzpb.ChannelRef{ChannelId: 1},
						Data:      &channelzpb.ChannelData{Target: "root"},
						SocketRef: []*channelzpb.SocketRef{{SocketId: 10}},
					},
				},
				End: true,
			},
		},
		sockets: map[int64]*channelzpb.Socket{
			10: {
				Ref:    &channelzpb.SocketRef{SocketId: 10},
				Local:  tcpAddress(net.ParseIP("127.0.0.1"), 1000),
				Remote: tcpAddress(net.ParseIP("127.0.0.2"), 2000),
				Data:   &channelzpb.SocketData{StreamsStarted: 1},
			},
		},
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{
		DisableLocalLabel:  true,
		DisableRemoteLabel: true,
	})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)
	mf := families["grpc_channelz_socket_streams_total"]
	require.NotNil(t, mf)
	requireMetricValue(t, mf, map[string]string{"id": "10", "type": "started"}, 1)
	require.Equal(t, []string{"id", "type"}, labelNames(mf.GetMetric()[0]))
}

func TestChannelzCollectorSkipsZeroStreamTimestamp(t *testing.T) {
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref:       &channelzpb.ChannelRef{ChannelId: 1},
						Data:      &channelzpb.ChannelData{Target: "root"},
						SocketRef: []*channelzpb.SocketRef{{SocketId: 10}},
					},
				},
				End: true,
			},
		},
		sockets: map[int64]*channelzpb.Socket{
			10: {
				Ref: &channelzpb.SocketRef{SocketId: 10},
				Data: &channelzpb.SocketData{
					StreamsStarted:                   1,
					LastLocalStreamCreatedTimestamp:  &timestamppb.Timestamp{},
					LastRemoteStreamCreatedTimestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
				},
			},
		},
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)

	require.Equal(t, 0, countMetrics(t, families["grpc_channelz_socket_last_stream_created_timestamp_seconds"], map[string]string{
		"id":   "10",
		"side": "local",
	}))
	require.Equal(t, 1, countMetrics(t, families["grpc_channelz_socket_last_stream_created_timestamp_seconds"], map[string]string{
		"id":   "10",
		"side": "remote",
	}))
}

func TestChannelzCollectorFetchErrors(t *testing.T) {
	server := &fakeChannelzServer{
		topChannels: map[int64]*channelzpb.GetTopChannelsResponse{
			0: {
				Channel: []*channelzpb.Channel{
					{
						Ref:           &channelzpb.ChannelRef{ChannelId: 1},
						Data:          &channelzpb.ChannelData{Target: "root", CallsStarted: 1},
						ChannelRef:    []*channelzpb.ChannelRef{{ChannelId: 2}},
						SubchannelRef: []*channelzpb.SubchannelRef{{SubchannelId: 3}},
						SocketRef:     []*channelzpb.SocketRef{{SocketId: 10}},
					},
				},
				End: true,
			},
		},
		channelErrors:    map[int64]error{2: status.Error(codes.Unavailable, "channel unavailable")},
		subchannelErrors: map[int64]error{3: status.Error(codes.Unavailable, "subchannel unavailable")},
		socketErrors:     map[int64]error{10: status.Error(codes.Unavailable, "socket unavailable")},
	}

	collector, cleanup := newTestCollector(t, server, ChannelzCollectorOpts{})
	defer cleanup()

	families := gatherMetricFamilies(t, collector)

	requireMetricValue(t, families["grpc_channelz_channel_calls_total"], map[string]string{
		"kind":   "channel",
		"id":     "1",
		"target": "root",
		"type":   "started",
	}, 1)
	requireMetricValue(t, families["grpc_channelz_fetch_errors_total"], map[string]string{"rpc": "GetTopChannels"}, 0)
	requireMetricValue(t, families["grpc_channelz_fetch_errors_total"], map[string]string{"rpc": "GetChannel"}, 1)
	requireMetricValue(t, families["grpc_channelz_fetch_errors_total"], map[string]string{"rpc": "GetSubchannel"}, 1)
	requireMetricValue(t, families["grpc_channelz_fetch_errors_total"], map[string]string{"rpc": "GetSocket"}, 1)
}

type fakeChannelzServer struct {
	channelzpb.UnimplementedChannelzServer

	topChannels      map[int64]*channelzpb.GetTopChannelsResponse
	topChannelErrors map[int64]error
	channels         map[int64]*channelzpb.Channel
	channelErrors    map[int64]error
	subchannels      map[int64]*channelzpb.Subchannel
	subchannelErrors map[int64]error
	sockets          map[int64]*channelzpb.Socket
	socketErrors     map[int64]error
}

func (s *fakeChannelzServer) GetTopChannels(_ context.Context, req *channelzpb.GetTopChannelsRequest) (*channelzpb.GetTopChannelsResponse, error) {
	if err := s.topChannelErrors[req.GetStartChannelId()]; err != nil {
		return nil, err
	}
	if resp := s.topChannels[req.GetStartChannelId()]; resp != nil {
		return resp, nil
	}
	return &channelzpb.GetTopChannelsResponse{End: true}, nil
}

func (s *fakeChannelzServer) GetChannel(_ context.Context, req *channelzpb.GetChannelRequest) (*channelzpb.GetChannelResponse, error) {
	if err := s.channelErrors[req.GetChannelId()]; err != nil {
		return nil, err
	}
	if channel := s.channels[req.GetChannelId()]; channel != nil {
		return &channelzpb.GetChannelResponse{Channel: channel}, nil
	}
	return nil, status.Error(codes.NotFound, "channel not found")
}

func (s *fakeChannelzServer) GetSubchannel(_ context.Context, req *channelzpb.GetSubchannelRequest) (*channelzpb.GetSubchannelResponse, error) {
	if err := s.subchannelErrors[req.GetSubchannelId()]; err != nil {
		return nil, err
	}
	if subchannel := s.subchannels[req.GetSubchannelId()]; subchannel != nil {
		return &channelzpb.GetSubchannelResponse{Subchannel: subchannel}, nil
	}
	return nil, status.Error(codes.NotFound, "subchannel not found")
}

func (s *fakeChannelzServer) GetSocket(_ context.Context, req *channelzpb.GetSocketRequest) (*channelzpb.GetSocketResponse, error) {
	if err := s.socketErrors[req.GetSocketId()]; err != nil {
		return nil, err
	}
	if socket := s.sockets[req.GetSocketId()]; socket != nil {
		return &channelzpb.GetSocketResponse{Socket: socket}, nil
	}
	return nil, status.Error(codes.NotFound, "socket not found")
}

func newTestCollector(t *testing.T, server *fakeChannelzServer, opts ChannelzCollectorOpts) (prometheus.Collector, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	channelzpb.RegisterChannelzServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	collector := NewChannelzCollector(conn, opts)
	return collector, func() {
		require.NoError(t, conn.Close())
		grpcServer.Stop()
		require.NoError(t, listener.Close())
	}
}

func gatherMetricFamilies(t *testing.T, collector prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector))

	families, err := registry.Gather()
	require.NoError(t, err)

	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func requireMetricValue(t *testing.T, family *dto.MetricFamily, labels map[string]string, want float64) {
	t.Helper()

	metric := findMetric(t, family, labels)
	require.InDelta(t, want, metricValue(t, family, metric), 0.000001)
}

func countMetrics(t *testing.T, family *dto.MetricFamily, labels map[string]string) int {
	t.Helper()

	if family == nil {
		return 0
	}
	count := 0
	for _, metric := range family.GetMetric() {
		if labelsMatch(metric, labels) {
			count++
		}
	}
	return count
}

func findMetric(t *testing.T, family *dto.MetricFamily, labels map[string]string) *dto.Metric {
	t.Helper()

	require.NotNil(t, family)
	for _, metric := range family.GetMetric() {
		if labelsMatch(metric, labels) {
			return metric
		}
	}
	require.FailNow(t, "metric not found", "family=%s labels=%v", family.GetName(), labels)
	return nil
}

func metricValue(t *testing.T, family *dto.MetricFamily, metric *dto.Metric) float64 {
	t.Helper()

	switch family.GetType() {
	case dto.MetricType_COUNTER:
		return metric.GetCounter().GetValue()
	case dto.MetricType_GAUGE:
		return metric.GetGauge().GetValue()
	default:
		require.FailNow(t, "unsupported metric family type", "family=%s type=%s", family.GetName(), family.GetType().String())
		return 0
	}
}

func labelsMatch(metric *dto.Metric, want map[string]string) bool {
	for key, value := range want {
		if labelValue(metric, key) != value {
			return false
		}
	}
	return true
}

func labelValue(metric *dto.Metric, key string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == key {
			return label.GetValue()
		}
	}
	return ""
}

func labelNames(metric *dto.Metric) []string {
	names := make([]string, 0, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		names = append(names, label.GetName())
	}
	sort.Strings(names)
	return names
}

func withLabels(labels map[string]string, key, value string) map[string]string {
	out := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out[key] = value
	return out
}

func tcpAddress(ip net.IP, port int32) *channelzpb.Address {
	return &channelzpb.Address{
		Address: &channelzpb.Address_TcpipAddress{
			TcpipAddress: &channelzpb.Address_TcpIpAddress{
				IpAddress: ip,
				Port:      port,
			},
		},
	}
}

func udsAddress(path string) *channelzpb.Address {
	return &channelzpb.Address{
		Address: &channelzpb.Address_UdsAddress_{
			UdsAddress: &channelzpb.Address_UdsAddress{Filename: path},
		},
	}
}

func otherAddress(name string) *channelzpb.Address {
	return &channelzpb.Address{
		Address: &channelzpb.Address_OtherAddress_{
			OtherAddress: &channelzpb.Address_OtherAddress{Name: name},
		},
	}
}
