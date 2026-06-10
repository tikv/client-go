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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	channelzpb "google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const channelzSubsystem = "grpc_channelz"

// ChannelzFilter decides whether the collector should emit metrics for the
// current node and whether it should keep walking into child nodes.
//
// The node is one of:
//   - *channelzpb.Channel
//   - *channelzpb.Subchannel
//   - *channelzpb.Socket
type ChannelzFilter func(node any) (collect bool, walkChildren bool)

// ChannelzCollectorOpts configures a channelz collector.
type ChannelzCollectorOpts struct {
	Namespace string
	Filter    ChannelzFilter

	// IncludeChannelTrace enables per-channel trace statistics.
	IncludeChannelTrace bool
	// IncludeChannelState enables the current per-channel connectivity state metric.
	IncludeChannelState bool

	// DisableLocalLabel suppresses the "local" label on socket metrics.
	DisableLocalLabel bool
	// DisableRemoteLabel suppresses the "remote" label on socket metrics.
	DisableRemoteLabel bool
}

// NewChannelzCollector constructs a Prometheus collector backed by the gRPC
// channelz API on the supplied client connection.
func NewChannelzCollector(conn *grpc.ClientConn, opts ChannelzCollectorOpts) prometheus.Collector {
	return newChannelzCollector(channelzpb.NewChannelzClient(conn), opts)
}

type channelzCollector struct {
	client              channelzpb.ChannelzClient
	filter              ChannelzFilter
	includeLocalLabel   bool
	includeRemoteLabel  bool
	includeChannelTrace bool
	includeChannelState bool

	channelCallsDesc             *prometheus.Desc
	channelLastCallStartedDesc   *prometheus.Desc
	channelStateDesc             *prometheus.Desc
	channelTraceEventsLoggedDesc *prometheus.Desc
	channelTraceCreationDesc     *prometheus.Desc
	channelTraceEventsDesc       *prometheus.Desc
	socketStreamsDesc            *prometheus.Desc
	socketMessagesDesc           *prometheus.Desc
	socketKeepAlivesDesc         *prometheus.Desc
	socketLastStreamCreatedDesc  *prometheus.Desc
	socketLastMessageDesc        *prometheus.Desc
	socketFlowControlWindowDesc  *prometheus.Desc
	fetchErrorsDesc              *prometheus.Desc
	descs                        []*prometheus.Desc
	getTopChannelsErrorsTotal    atomic.Uint64
	getChannelErrorsTotal        atomic.Uint64
	getSubchannelErrorsTotal     atomic.Uint64
	getSocketErrorsTotal         atomic.Uint64
}

func newChannelzCollector(client channelzpb.ChannelzClient, opts ChannelzCollectorOpts) *channelzCollector {
	c := &channelzCollector{
		client:              client,
		filter:              opts.Filter,
		includeLocalLabel:   !opts.DisableLocalLabel,
		includeRemoteLabel:  !opts.DisableRemoteLabel,
		includeChannelTrace: opts.IncludeChannelTrace,
		includeChannelState: opts.IncludeChannelState,
	}

	channelLabels := []string{"kind", "id", "target"}
	socketLabels := c.socketLabelNames()

	c.channelCallsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_calls_total"),
		"Total calls observed by the channelz channel or subchannel.",
		appendLabels(channelLabels, "type"),
		nil,
	)
	c.channelLastCallStartedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_last_call_started_timestamp_seconds"),
		"Unix timestamp of the last call started on the channelz channel or subchannel.",
		channelLabels,
		nil,
	)
	c.socketStreamsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_streams_total"),
		"Total streams observed by the channelz socket.",
		appendLabels(socketLabels, "type"),
		nil,
	)
	c.socketMessagesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_messages_total"),
		"Total messages observed by the channelz socket.",
		appendLabels(socketLabels, "direction"),
		nil,
	)
	c.socketKeepAlivesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_keepalives_total"),
		"Total keepalive pings sent on the channelz socket.",
		socketLabels,
		nil,
	)
	c.socketLastStreamCreatedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_last_stream_created_timestamp_seconds"),
		"Unix timestamp of the last stream created on the channelz socket.",
		appendLabels(socketLabels, "side"),
		nil,
	)
	c.socketLastMessageDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_last_message_timestamp_seconds"),
		"Unix timestamp of the last message activity observed on the channelz socket.",
		appendLabels(socketLabels, "direction"),
		nil,
	)
	c.socketFlowControlWindowDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "socket_flow_control_window_bytes"),
		"HTTP/2 flow control window exposed by the channelz socket.",
		appendLabels(socketLabels, "side"),
		nil,
	)
	c.fetchErrorsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "fetch_errors_total"),
		"Total RPC fetch errors encountered by the channelz collector.",
		[]string{"rpc"},
		nil,
	)
	c.descs = []*prometheus.Desc{
		c.channelCallsDesc,
		c.channelLastCallStartedDesc,
		c.socketStreamsDesc,
		c.socketMessagesDesc,
		c.socketKeepAlivesDesc,
		c.socketLastStreamCreatedDesc,
		c.socketLastMessageDesc,
		c.socketFlowControlWindowDesc,
		c.fetchErrorsDesc,
	}
	if c.includeChannelState {
		c.channelStateDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_state"),
			"Connectivity state of the channelz channel or subchannel, exported as a one-hot gauge.",
			appendLabels(channelLabels, "state"),
			nil,
		)
		c.descs = append(c.descs, c.channelStateDesc)
	}
	if c.includeChannelTrace {
		c.channelTraceEventsLoggedDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_trace_events_logged_total"),
			"Total number of events ever logged in the channelz trace object.",
			channelLabels,
			nil,
		)
		c.channelTraceCreationDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_trace_creation_timestamp_seconds"),
			"Unix timestamp when the channelz trace object was created.",
			channelLabels,
			nil,
		)
		c.channelTraceEventsDesc = prometheus.NewDesc(
			prometheus.BuildFQName(opts.Namespace, channelzSubsystem, "channel_trace_event_count"),
			"Current number of trace events in the channelz trace buffer, partitioned by severity.",
			appendLabels(channelLabels, "severity"),
			nil,
		)
		c.descs = append(c.descs, c.channelTraceEventsLoggedDesc, c.channelTraceCreationDesc, c.channelTraceEventsDesc)
	}
	return c
}

func (c *channelzCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *channelzCollector) Collect(ch chan<- prometheus.Metric) {
	w := channelzWalker{
		collector:       c,
		ch:              ch,
		now:             time.Now(),
		seenChannels:    make(map[int64]struct{}),
		seenSubchannels: make(map[int64]struct{}),
		seenSockets:     make(map[int64]struct{}),
	}
	w.walkTopChannels()
	c.collectFetchErrorMetrics(ch)
}

type channelzWalker struct {
	collector       *channelzCollector
	ch              chan<- prometheus.Metric
	now             time.Time
	seenChannels    map[int64]struct{}
	seenSubchannels map[int64]struct{}
	seenSockets     map[int64]struct{}
}

func (w *channelzWalker) walkTopChannels() {
	var startID int64
	for {
		resp, err := w.collector.client.GetTopChannels(context.Background(), &channelzpb.GetTopChannelsRequest{
			StartChannelId: startID,
			MaxResults:     0,
		})
		if err != nil {
			w.collector.getTopChannelsErrorsTotal.Add(1)
			return
		}

		var maxID int64
		for _, channel := range resp.GetChannel() {
			if id := channel.GetRef().GetChannelId(); id > maxID {
				maxID = id
			}
			w.walkChannel(channel)
		}

		if resp.GetEnd() || len(resp.GetChannel()) == 0 || maxID <= startID {
			return
		}
		startID = maxID + 1
	}
}

func (w *channelzWalker) walkChannel(channel *channelzpb.Channel) {
	if channel == nil {
		return
	}
	id := channel.GetRef().GetChannelId()
	if id <= 0 {
		return
	}
	if _, ok := w.seenChannels[id]; ok {
		return
	}
	w.seenChannels[id] = struct{}{}

	collect, walkChildren := w.collector.applyFilter(channel)
	if collect {
		w.collectChannelMetrics("channel", id, channel.GetData())
	}
	if !walkChildren {
		return
	}

	for _, ref := range channel.GetChannelRef() {
		child, err := w.collector.client.GetChannel(context.Background(), &channelzpb.GetChannelRequest{ChannelId: ref.GetChannelId()})
		if err != nil {
			w.collector.getChannelErrorsTotal.Add(1)
			continue
		}
		w.walkChannel(child.GetChannel())
	}
	for _, ref := range channel.GetSubchannelRef() {
		child, err := w.collector.client.GetSubchannel(context.Background(), &channelzpb.GetSubchannelRequest{SubchannelId: ref.GetSubchannelId()})
		if err != nil {
			w.collector.getSubchannelErrorsTotal.Add(1)
			continue
		}
		w.walkSubchannel(child.GetSubchannel())
	}
	for _, ref := range channel.GetSocketRef() {
		socket, err := w.collector.client.GetSocket(context.Background(), &channelzpb.GetSocketRequest{SocketId: ref.GetSocketId()})
		if err != nil {
			w.collector.getSocketErrorsTotal.Add(1)
			continue
		}
		w.walkSocket(socket.GetSocket())
	}
}

func (w *channelzWalker) walkSubchannel(subchannel *channelzpb.Subchannel) {
	if subchannel == nil {
		return
	}
	id := subchannel.GetRef().GetSubchannelId()
	if id <= 0 {
		return
	}
	if _, ok := w.seenSubchannels[id]; ok {
		return
	}
	w.seenSubchannels[id] = struct{}{}

	collect, walkChildren := w.collector.applyFilter(subchannel)
	if collect {
		w.collectChannelMetrics("subchannel", id, subchannel.GetData())
	}
	if !walkChildren {
		return
	}

	for _, ref := range subchannel.GetChannelRef() {
		child, err := w.collector.client.GetChannel(context.Background(), &channelzpb.GetChannelRequest{ChannelId: ref.GetChannelId()})
		if err != nil {
			w.collector.getChannelErrorsTotal.Add(1)
			continue
		}
		w.walkChannel(child.GetChannel())
	}
	for _, ref := range subchannel.GetSubchannelRef() {
		child, err := w.collector.client.GetSubchannel(context.Background(), &channelzpb.GetSubchannelRequest{SubchannelId: ref.GetSubchannelId()})
		if err != nil {
			w.collector.getSubchannelErrorsTotal.Add(1)
			continue
		}
		w.walkSubchannel(child.GetSubchannel())
	}
	for _, ref := range subchannel.GetSocketRef() {
		socket, err := w.collector.client.GetSocket(context.Background(), &channelzpb.GetSocketRequest{SocketId: ref.GetSocketId()})
		if err != nil {
			w.collector.getSocketErrorsTotal.Add(1)
			continue
		}
		w.walkSocket(socket.GetSocket())
	}
}

func (w *channelzWalker) walkSocket(socket *channelzpb.Socket) {
	if socket == nil {
		return
	}
	id := socket.GetRef().GetSocketId()
	if id <= 0 {
		return
	}
	if _, ok := w.seenSockets[id]; ok {
		return
	}
	w.seenSockets[id] = struct{}{}

	collect, _ := w.collector.applyFilter(socket)
	if collect {
		w.collectSocketMetrics(id, socket)
	}
}

func (w *channelzWalker) collectChannelMetrics(kind string, id int64, data *channelzpb.ChannelData) {
	if data == nil {
		return
	}
	labels := []string{kind, strconv.FormatInt(id, 10), data.GetTarget()}

	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelCallsDesc), prometheus.CounterValue, float64(data.GetCallsStarted()), appendLabels(labels, "started")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelCallsDesc), prometheus.CounterValue, float64(data.GetCallsSucceeded()), appendLabels(labels, "succeeded")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelCallsDesc), prometheus.CounterValue, float64(data.GetCallsFailed()), appendLabels(labels, "failed")...)

	if ts := data.GetLastCallStartedTimestamp(); ts != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelLastCallStartedDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), labels...)
	}

	if w.collector.includeChannelState {
		currentState := normalizeConnectivityState(data.GetState())
		for _, state := range []string{"unknown", "idle", "connecting", "ready", "transient_failure", "shutdown"} {
			value := 0.0
			if state == currentState {
				value = 1
			}
			w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelStateDesc), prometheus.GaugeValue, value, appendLabels(labels, state)...)
		}
	}

	if !w.collector.includeChannelTrace {
		return
	}
	trace := data.GetTrace()
	if trace == nil {
		return
	}
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelTraceEventsLoggedDesc), prometheus.CounterValue, float64(trace.GetNumEventsLogged()), labels...)
	if ts := trace.GetCreationTimestamp(); ts != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelTraceCreationDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), labels...)
	}

	severityCounts := map[string]float64{
		"unknown": 0,
		"info":    0,
		"warning": 0,
		"error":   0,
	}
	for _, event := range trace.GetEvents() {
		severityCounts[normalizeSeverity(event.GetSeverity())]++
	}
	for _, severity := range []string{"unknown", "info", "warning", "error"} {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.channelTraceEventsDesc), prometheus.GaugeValue, severityCounts[severity], appendLabels(labels, severity)...)
	}
}

func (w *channelzWalker) collectSocketMetrics(id int64, socket *channelzpb.Socket) {
	data := socket.GetData()
	if data == nil {
		return
	}
	labels := w.collector.socketLabelValues(id, socket)

	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketStreamsDesc), prometheus.CounterValue, float64(data.GetStreamsStarted()), appendLabels(labels, "started")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketStreamsDesc), prometheus.CounterValue, float64(data.GetStreamsSucceeded()), appendLabels(labels, "succeeded")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketStreamsDesc), prometheus.CounterValue, float64(data.GetStreamsFailed()), appendLabels(labels, "failed")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketMessagesDesc), prometheus.CounterValue, float64(data.GetMessagesSent()), appendLabels(labels, "sent")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketMessagesDesc), prometheus.CounterValue, float64(data.GetMessagesReceived()), appendLabels(labels, "received")...)
	w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketKeepAlivesDesc), prometheus.CounterValue, float64(data.GetKeepAlivesSent()), labels...)

	if ts := data.GetLastLocalStreamCreatedTimestamp(); hasUsableTimestamp(ts) {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketLastStreamCreatedDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), appendLabels(labels, "local")...)
	}
	if ts := data.GetLastRemoteStreamCreatedTimestamp(); hasUsableTimestamp(ts) {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketLastStreamCreatedDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), appendLabels(labels, "remote")...)
	}
	if ts := data.GetLastMessageSentTimestamp(); ts != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketLastMessageDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), appendLabels(labels, "sent")...)
	}
	if ts := data.GetLastMessageReceivedTimestamp(); ts != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketLastMessageDesc), prometheus.GaugeValue, timestampSeconds(ts.AsTime()), appendLabels(labels, "received")...)
	}
	if window := data.GetLocalFlowControlWindow(); window != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketFlowControlWindowDesc), prometheus.GaugeValue, float64(window.GetValue()), appendLabels(labels, "local")...)
	}
	if window := data.GetRemoteFlowControlWindow(); window != nil {
		w.ch <- prometheus.MustNewConstMetric(cDesc(w.collector.socketFlowControlWindowDesc), prometheus.GaugeValue, float64(window.GetValue()), appendLabels(labels, "remote")...)
	}
}

func (c *channelzCollector) socketLabelNames() []string {
	labels := []string{"id"}
	if c.includeLocalLabel {
		labels = append(labels, "local")
	}
	if c.includeRemoteLabel {
		labels = append(labels, "remote")
	}
	return labels
}

func (c *channelzCollector) socketLabelValues(id int64, socket *channelzpb.Socket) []string {
	values := []string{strconv.FormatInt(id, 10)}
	if c.includeLocalLabel {
		values = append(values, formatAddress(socket.GetLocal()))
	}
	if c.includeRemoteLabel {
		values = append(values, formatAddress(socket.GetRemote()))
	}
	return values
}

func (c *channelzCollector) applyFilter(node any) (collect bool, walkChildren bool) {
	if c.filter == nil {
		return true, true
	}
	return c.filter(node)
}

func (c *channelzCollector) collectFetchErrorMetrics(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.fetchErrorsDesc, prometheus.CounterValue, float64(c.getTopChannelsErrorsTotal.Load()), "GetTopChannels")
	ch <- prometheus.MustNewConstMetric(c.fetchErrorsDesc, prometheus.CounterValue, float64(c.getChannelErrorsTotal.Load()), "GetChannel")
	ch <- prometheus.MustNewConstMetric(c.fetchErrorsDesc, prometheus.CounterValue, float64(c.getSubchannelErrorsTotal.Load()), "GetSubchannel")
	ch <- prometheus.MustNewConstMetric(c.fetchErrorsDesc, prometheus.CounterValue, float64(c.getSocketErrorsTotal.Load()), "GetSocket")
}

func appendLabels(labels []string, extra string) []string {
	out := make([]string, 0, len(labels)+1)
	out = append(out, labels...)
	out = append(out, extra)
	return out
}

func cDesc(desc *prometheus.Desc) *prometheus.Desc {
	return desc
}

func timestampSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}

func hasUsableTimestamp(ts *timestamppb.Timestamp) bool {
	return ts != nil && ts.IsValid() && (ts.Seconds != 0 || ts.Nanos != 0)
}

func normalizeConnectivityState(state *channelzpb.ChannelConnectivityState) string {
	if state == nil {
		return "unknown"
	}
	switch state.GetState() {
	case channelzpb.ChannelConnectivityState_IDLE:
		return "idle"
	case channelzpb.ChannelConnectivityState_CONNECTING:
		return "connecting"
	case channelzpb.ChannelConnectivityState_READY:
		return "ready"
	case channelzpb.ChannelConnectivityState_TRANSIENT_FAILURE:
		return "transient_failure"
	case channelzpb.ChannelConnectivityState_SHUTDOWN:
		return "shutdown"
	default:
		return "unknown"
	}
}

func normalizeSeverity(severity channelzpb.ChannelTraceEvent_Severity) string {
	switch severity {
	case channelzpb.ChannelTraceEvent_CT_INFO:
		return "info"
	case channelzpb.ChannelTraceEvent_CT_WARNING:
		return "warning"
	case channelzpb.ChannelTraceEvent_CT_ERROR:
		return "error"
	default:
		return "unknown"
	}
}

func formatAddress(addr *channelzpb.Address) string {
	if addr == nil {
		return ""
	}
	if tcpAddr := addr.GetTcpipAddress(); tcpAddr != nil {
		ip := net.IP(tcpAddr.GetIpAddress()).String()
		if tcpAddr.GetPort() < 0 {
			return ip
		}
		return net.JoinHostPort(ip, strconv.Itoa(int(tcpAddr.GetPort())))
	}
	if udsAddr := addr.GetUdsAddress(); udsAddr != nil {
		return udsAddr.GetFilename()
	}
	if otherAddr := addr.GetOtherAddress(); otherAddr != nil {
		return otherAddr.GetName()
	}
	return ""
}
