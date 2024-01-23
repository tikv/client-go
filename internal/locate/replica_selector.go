package locate

import (
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type ReplicaSelector interface {
	next(bo *retry.Backoffer) (*RPCContext, error)
	targetReplica() *replica
	proxyReplica() *replica
	replicaType(rpcCtx *RPCContext) string
	invalidateRegion()
	onSendSuccess()
	String() string
	onSendFailure(bo *retry.Backoffer, err error)
	// Following methods are used to handle region errors.
	onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error)
	onFlashbackInProgress()
	onServerIsBusy(bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy) (shouldRetry bool, err error)
	onReadReqConfigurableTimeout(req *tikvrpc.Request) bool
}

type replicaSelectorV2 struct {
}

func NewreplicaSelectorV2(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (*replicaSelectorV2, error) {

	return &replicaSelectorV2{}, nil
}

func (rs *replicaSelectorV2) next(bo *retry.Backoffer) (*RPCContext, error) {
	return nil, nil
}

func (rs *replicaSelectorV2) onNotLeader(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {

	return false, nil
}

func (rs *replicaSelectorV2) onFlashbackInProgress(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {

	return false, nil
}

func (rs *replicaSelectorV2) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	return false, err
}

func (rs *replicaSelectorV2) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
	return false
}

func (rs *replicaSelectorV2) onSendFailure(bo *retry.Backoffer, err error) {
}

func (rs *replicaSelectorV2) onSendSuccess() {
}

func (rs *replicaSelectorV2) recordAttemptedTime(duration time.Duration) {
}

func (rs *replicaSelectorV2) targetReplica() *replica {
	return nil
}

func (rs *replicaSelectorV2) replicaType(rpcCtx *RPCContext) string {
	return ""
}

func (rs *replicaSelectorV2) invalidateRegion() {
}

func (rs *replicaSelectorV2) String() string {
	return ""
}

type replicaSelectorV2V2 struct {
}
