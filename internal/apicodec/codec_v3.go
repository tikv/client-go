package apicodec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
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

// codecV3 uses API V3 request context for TiKV RPCs. During the current
// keyspace-boundary rollout, PD region bounds remain aligned to the bare
// keyspace-id prefix, so region routing uses routePrefix instead of the full
// namespace-aware identity prefix.
type codecV3 struct {
	*codecV2
	routePrefix []byte
	routeEndKey []byte
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
	routePrefix := make([]byte, keyspacePrefixLen)
	switch mode {
	case ModeRaw:
		prefix[0] = RawModePrefix
		routePrefix[0] = RawModePrefix
	case ModeTxn:
		prefix[0] = TxnModePrefix
		routePrefix[0] = TxnModePrefix
	default:
		return nil, errors.Errorf("unknown mode")
	}
	binary.BigEndian.PutUint32(prefix[1:5], namespaceID)
	keyspaceIDBytes, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}
	copy(prefix[5:], keyspaceIDBytes)
	copy(routePrefix[1:], keyspaceIDBytes)

	endKey := make([]byte, apiV3KeyspacePrefixLen)
	prefixVal := binary.BigEndian.Uint64(prefix)
	binary.BigEndian.PutUint64(endKey, prefixVal+1)
	routeEndKey := make([]byte, keyspacePrefixLen)
	routePrefixVal := binary.BigEndian.Uint32(routePrefix)
	binary.BigEndian.PutUint32(routeEndKey, routePrefixVal+1)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Name:     BuildKeyspaceName(keyspaceName),
		Keyspace: &keyspacepb.KeyspaceMeta_KeyspaceIdentity{KeyspaceIdentity: identity},
	}
	base := &codecV2{
		apiVersion:   kvrpcpb.APIVersion_V3,
		prefix:       prefix,
		endKey:       endKey,
		memCodec:     &memComparableCodec{},
		keyspaceMeta: keyspaceMeta,
	}
	base.reqPool.New = func() any { return &tikvrpc.Request{} }
	return &codecV3{codecV2: base, routePrefix: routePrefix, routeEndKey: routeEndKey}, nil
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

func (c *codecV3) EncodeRegionKey(key []byte) []byte {
	return c.memCodec.encodeKey(c.encodeRegionKey(key))
}

func (c *codecV3) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	memDecoded, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.decodeRegionKey(memDecoded)
}

func (c *codecV3) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedStart, encodedEnd := c.encodeRegionRange(start, end)
	return c.memCodec.encodeKey(encodedStart), c.memCodec.encodeKey(encodedEnd)
}

func (c *codecV3) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
	var err error
	if len(encodedStart) != 0 {
		encodedStart, err = c.memCodec.decodeKey(encodedStart)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(encodedEnd) != 0 {
		encodedEnd, err = c.memCodec.decodeKey(encodedEnd)
		if err != nil {
			return nil, nil, err
		}
	}
	return c.decodeRegionRange(encodedStart, encodedEnd)
}

func (c *codecV3) encodeRegionKey(key []byte) []byte {
	return append(append([]byte{}, c.routePrefix...), key...)
}

func (c *codecV3) encodeRegionRange(start, end []byte) ([]byte, []byte) {
	var encodedEnd []byte
	if len(end) > 0 {
		encodedEnd = c.encodeRegionKey(end)
	} else {
		encodedEnd = c.routeEndKey
	}
	return c.encodeRegionKey(start), encodedEnd
}

func (c *codecV3) decodeRegionKey(encodedKey []byte) ([]byte, error) {
	if len(encodedKey) == 0 {
		return nil, nil
	}
	if bytes.HasPrefix(encodedKey, c.routePrefix) {
		return encodedKey[len(c.routePrefix):], nil
	}
	if bytes.HasPrefix(encodedKey, c.prefix) {
		return encodedKey[len(c.prefix):], nil
	}
	if c.isLogicalRegionKey(encodedKey) {
		return append([]byte{}, encodedKey...), nil
	}
	return nil, errKeyOutOfBound
}

func (c *codecV3) decodeRegionRange(encodedStart, encodedEnd []byte) (start []byte, end []byte, err error) {
	if c.isLogicalRegionRange(encodedStart, encodedEnd) {
		return append([]byte{}, encodedStart...), append([]byte{}, encodedEnd...), nil
	}

	if bytes.Compare(encodedStart, c.routeEndKey) >= 0 ||
		(len(encodedEnd) > 0 && bytes.Compare(encodedEnd, c.routePrefix) <= 0) {
		return nil, nil, errors.WithStack(errKeyOutOfBound)
	}

	start, end = []byte{}, []byte{}
	if len(encodedStart) > 0 {
		start, err = c.decodeRegionKey(encodedStart)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(encodedEnd) > 0 && !bytes.Equal(encodedEnd, c.routeEndKey) {
		end, err = c.decodeRegionKey(encodedEnd)
		if err != nil {
			return nil, nil, err
		}
	}
	return start, end, nil
}

func (c *codecV3) isLogicalRegionRange(start, end []byte) bool {
	return c.isLogicalRegionKey(start) && c.isLogicalRegionKey(end)
}

func (c *codecV3) isLogicalRegionKey(key []byte) bool {
	if len(key) == 0 {
		return true
	}
	if bytes.HasPrefix(key, c.routePrefix) || bytes.HasPrefix(key, c.prefix) {
		return false
	}
	// Until API V3 region boundaries become namespace-aware, PD may answer a
	// keyspace-scoped region request with logical keys. Keep accepting physical
	// looking keys as out-of-bound so stale cross-keyspace regions are not
	// silently treated as user keys.
	return len(key) < keyspacePrefixLen || (key[0] != RawModePrefix && key[0] != TxnModePrefix)
}

func (c *codecV3) decodeRegionError(regionError *errorpb.Error) (*errorpb.Error, error) {
	if regionError == nil {
		return nil, nil
	}
	var err error
	if errInfo := regionError.KeyNotInRegion; errInfo != nil {
		if len(errInfo.Key) > 0 {
			errInfo.Key, err = c.decodeRegionKey(errInfo.Key)
			if err != nil {
				return nil, err
			}
		}
		errInfo.StartKey, errInfo.EndKey, err = c.DecodeRegionRange(errInfo.StartKey, errInfo.EndKey)
		if err != nil {
			return nil, err
		}
	}

	if errInfo := regionError.EpochNotMatch; errInfo != nil {
		decodedRegions := make([]*metapb.Region, 0, len(errInfo.CurrentRegions))
		for _, meta := range errInfo.CurrentRegions {
			meta.StartKey, meta.EndKey, err = c.DecodeRegionRange(meta.StartKey, meta.EndKey)
			if err != nil {
				if errors.Is(err, errKeyOutOfBound) {
					continue
				}
				return nil, err
			}
			decodedRegions = append(decodedRegions, meta)
		}
		errInfo.CurrentRegions = decodedRegions
	}

	return regionError, nil
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
