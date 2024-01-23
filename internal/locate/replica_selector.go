package locate

import (
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type ReplicaSelector struct {
	//v1 *replicaSelector
}

func newReplicaSelector(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (*ReplicaSelector, error) {

	return &ReplicaSelector{}, nil
}

func (rs *ReplicaSelector) next(bo *retry.Backoffer) (*RPCContext, error) {
	return nil, nil
}

func (rs *ReplicaSelector) onNotLeader(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {

	return false, nil
}

func (rs *ReplicaSelector) onFlashbackInProgress(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {

	return false, nil
}

func (rs *ReplicaSelector) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	return false, err
}

func (rs *ReplicaSelector) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
	return false
}

func (rs *ReplicaSelector) onSendFailure(bo *retry.Backoffer, err error) {
}

func (rs *ReplicaSelector) targetReplica() *replica {
	return nil
}

func (rs *ReplicaSelector) replicaType(rpcCtx *RPCContext) string {
	return ""
}

func (rs *ReplicaSelector) invalidateRegion() {
}

func (rs *ReplicaSelector) String() string {
	return ""
}

type replicaSelectorV2 struct {
}
