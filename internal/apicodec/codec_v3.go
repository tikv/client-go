package apicodec

import (
	"encoding/binary"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const apiV3KeyspacePrefixLen = 8

func checkV3Key(b []byte) error {
	if len(b) < apiV3KeyspacePrefixLen || (b[0] != RawModePrefix && b[0] != TxnModePrefix) {
		return errors.Errorf("invalid API V3 key %s", b)
	}
	return nil
}

// codecV3 uses API V3 request context for TiKV RPCs and API V3 physical
// prefixes for PD region lookups.
type codecV3 struct {
	*codecV2
}

// NewCodecV3 returns a codec for API V3 tenant-scoped keyspaces.
func NewCodecV3(mode Mode, identity *apipb.KeyspaceIdentity, keyspaceName string) (Codec, error) {
	if identity == nil {
		return nil, errors.New("missing API V3 keyspace identity")
	}
	namespaceID := identity.GetNamespaceId()
	keyspaceID := identity.GetKeyspaceId()
	if namespaceID == 0 {
		return nil, errors.New("API V3 namespaceID must be non-zero")
	}
	if keyspaceID == 0 || keyspaceID > maxKeyspaceID {
		return nil, errors.Errorf("API V3 keyspaceID %d is out of range, valid range is [1, %d]", keyspaceID, maxKeyspaceID)
	}

	prefix := make([]byte, apiV3KeyspacePrefixLen)
	switch mode {
	case ModeRaw:
		prefix[0] = RawModePrefix
	case ModeTxn:
		prefix[0] = TxnModePrefix
	default:
		return nil, errors.Errorf("unknown mode")
	}
	binary.BigEndian.PutUint32(prefix[1:5], namespaceID)
	keyspaceIDBytes, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}
	copy(prefix[5:], keyspaceIDBytes)

	endKey := make([]byte, apiV3KeyspacePrefixLen)
	prefixVal := binary.BigEndian.Uint64(prefix)
	binary.BigEndian.PutUint64(endKey, prefixVal+1)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Name:             BuildKeyspaceName(keyspaceName),
		Id:               keyspaceID,
		KeyspaceIdentity: identity,
	}
	base := &codecV2{
		apiVersion:   kvrpcpb.APIVersion_V3,
		prefix:       prefix,
		endKey:       endKey,
		memCodec:     &memComparableCodec{},
		keyspaceMeta: keyspaceMeta,
	}
	base.reqPool.New = func() any { return &tikvrpc.Request{} }
	return &codecV3{codecV2: base}, nil
}

// EncodeRequest keeps V3 RPC key fields as user keys. TiKV uses the V3
// keyspace identity in request context to apply the physical keyspace prefix.
func (c *codecV3) EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error) {
	r := c.reqPool.Get().(*tikvrpc.Request)
	*r = *req
	setAPICtx(c, r)
	return r, nil
}

func (c *codecV3) DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	defer c.reqPool.Put(req)
	regionError, err := resp.GetRegionError()
	if err != nil {
		return resp, nil
	}
	decodedRegionError, err := c.decodeRegionError(regionError)
	if err != nil {
		return nil, err
	}
	setRegionError(resp, decodedRegionError)
	return resp, nil
}

func setRegionError(resp *tikvrpc.Response, regionError *errorpb.Error) {
	switch r := resp.Resp.(type) {
	case *kvrpcpb.GetResponse:
		r.RegionError = regionError
	case *kvrpcpb.ScanResponse:
		r.RegionError = regionError
	case *kvrpcpb.PrewriteResponse:
		r.RegionError = regionError
	case *kvrpcpb.CommitResponse:
		r.RegionError = regionError
	case *kvrpcpb.CleanupResponse:
		r.RegionError = regionError
	case *kvrpcpb.BatchGetResponse:
		r.RegionError = regionError
	case *kvrpcpb.BatchRollbackResponse:
		r.RegionError = regionError
	case *kvrpcpb.ScanLockResponse:
		r.RegionError = regionError
	case *kvrpcpb.ResolveLockResponse:
		r.RegionError = regionError
	case *kvrpcpb.GCResponse:
		r.RegionError = regionError
	case *kvrpcpb.DeleteRangeResponse:
		r.RegionError = regionError
	case *kvrpcpb.PessimisticLockResponse:
		r.RegionError = regionError
	case *kvrpcpb.PessimisticRollbackResponse:
		r.RegionError = regionError
	case *kvrpcpb.TxnHeartBeatResponse:
		r.RegionError = regionError
	case *kvrpcpb.CheckTxnStatusResponse:
		r.RegionError = regionError
	case *kvrpcpb.CheckSecondaryLocksResponse:
		r.RegionError = regionError
	case *kvrpcpb.FlushResponse:
		r.RegionError = regionError
	case *kvrpcpb.BufferBatchGetResponse:
		r.RegionError = regionError
	case *kvrpcpb.FlashbackToVersionResponse:
		r.RegionError = regionError
	case *kvrpcpb.PrepareFlashbackToVersionResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawGetResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawBatchGetResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawPutResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawBatchPutResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawDeleteResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawBatchDeleteResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawDeleteRangeResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawScanResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawGetKeyTTLResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawCASResponse:
		r.RegionError = regionError
	case *kvrpcpb.RawChecksumResponse:
		r.RegionError = regionError
	case *kvrpcpb.UnsafeDestroyRangeResponse:
		r.RegionError = regionError
	case *kvrpcpb.MvccGetByKeyResponse:
		r.RegionError = regionError
	case *kvrpcpb.MvccGetByStartTsResponse:
		r.RegionError = regionError
	case *kvrpcpb.SplitRegionResponse:
		r.RegionError = regionError
	}
}
