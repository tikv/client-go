package apicodec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

var (
	// DefaultKeyspaceID is the keyspaceID of the default keyspace.
	DefaultKeyspaceID uint32 = 0
	// DefaultKeyspaceName is the name of the default keyspace.
	DefaultKeyspaceName = "DEFAULT"

	rawModePrefix     byte = 'r'
	txnModePrefix     byte = 'x'
	keyspacePrefixLen      = 4

	// errKeyOutOfBound happens when key to be decoded lies outside the keyspace's range.
	errKeyOutOfBound = errors.New("given key does not belong to the keyspace")
)

// BuildKeyspaceName builds a keyspace name
func BuildKeyspaceName(name string) string {
	if name == "" {
		return DefaultKeyspaceName
	}
	return name
}

// codecV2 is used to encode/decode keys and request into APIv2 format.
type codecV2 struct {
	prefix   []byte
	endKey   []byte
	memCodec memCodec
}

// NewCodecV2 returns a codec that can be used to encode/decode
// keys and requests to and from APIv2 format.
func NewCodecV2(mode Mode, keyspaceID uint32) (Codec, error) {
	prefix, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}

	// Region keys in CodecV2 are always encoded in memory comparable form.
	codec := &codecV2{memCodec: &memComparableCodec{}}
	codec.prefix = make([]byte, 4)
	codec.endKey = make([]byte, 4)
	switch mode {
	case ModeRaw:
		codec.prefix[0] = rawModePrefix
	case ModeTxn:
		codec.prefix[0] = txnModePrefix
	default:
		return nil, errors.Errorf("unknown mode")
	}
	copy(codec.prefix[1:], prefix)
	prefixVal := binary.BigEndian.Uint32(codec.prefix)
	binary.BigEndian.PutUint32(codec.endKey, prefixVal+1)
	return codec, nil
}

func getIDByte(keyspaceID uint32) ([]byte, error) {
	// PutUint32 requires 4 bytes to operate, so must use buffer with size 4 here.
	b := make([]byte, 4)
	// Use BigEndian to put the least significant byte to last array position.
	// For example, keyspaceID 1 should result in []byte{0, 0, 1}
	binary.BigEndian.PutUint32(b, keyspaceID)
	// When keyspaceID can't fit in 3 bytes, first byte of buffer will be non-zero.
	// So return error.
	if b[0] != 0 {
		return nil, errors.Errorf("illegal keyspaceID: %v, keyspaceID must be 3 byte", b)
	}
	// Remove the first byte to make keyspace ID 3 bytes.
	return b[1:], nil
}

func (c *codecV2) GetKeyspace() []byte {
	return c.prefix
}

func (c *codecV2) GetKeyspaceID() KeyspaceID {
	prefix := append([]byte{}, c.prefix...)
	prefix[0] = 0
	return KeyspaceID(binary.BigEndian.Uint32(prefix))
}

func (c *codecV2) GetAPIVersion() kvrpcpb.APIVersion {
	return kvrpcpb.APIVersion_V2
}

// EncodeRequest encodes with the given Codec.
// NOTE: req is reused on retry. MUST encode on cloned request, other than overwrite the original.
func (c *codecV2) EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error) {
	newReq := *req
	// Encode requests based on command type.
	switch req.Type {
	// Transaction Request Types.
	case tikvrpc.CmdGet:
		r := *req.Get()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdScan:
		r := *req.Scan()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, r.Reverse)
		newReq.Req = &r
	case tikvrpc.CmdPrewrite:
		r := *req.Prewrite()
		r.Mutations = c.encodeMutations(r.Mutations)
		r.PrimaryLock = c.EncodeKey(r.PrimaryLock)
		r.Secondaries = c.encodeKeys(r.Secondaries)
		newReq.Req = &r
	case tikvrpc.CmdCommit:
		r := *req.Commit()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdCleanup:
		r := *req.Cleanup()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdBatchGet:
		r := *req.BatchGet()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdBatchRollback:
		r := *req.BatchRollback()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdScanLock:
		r := *req.ScanLock()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, false)
		newReq.Req = &r
	case tikvrpc.CmdResolveLock:
		r := *req.ResolveLock()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdGC:
		// TODO: Deprecate Central GC Mode.
	case tikvrpc.CmdDeleteRange:
		r := *req.DeleteRange()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, false)
		newReq.Req = &r
	case tikvrpc.CmdPessimisticLock:
		r := *req.PessimisticLock()
		r.Mutations = c.encodeMutations(r.Mutations)
		r.PrimaryLock = c.EncodeKey(r.PrimaryLock)
		newReq.Req = &r
	case tikvrpc.CmdPessimisticRollback:
		r := *req.PessimisticRollback()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdTxnHeartBeat:
		r := *req.TxnHeartBeat()
		r.PrimaryLock = c.EncodeKey(r.PrimaryLock)
		newReq.Req = &r
	case tikvrpc.CmdCheckTxnStatus:
		r := *req.CheckTxnStatus()
		r.PrimaryKey = c.EncodeKey(r.PrimaryKey)
		newReq.Req = &r
	case tikvrpc.CmdCheckSecondaryLocks:
		r := *req.CheckSecondaryLocks()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r

	// Raw Request Types.
	case tikvrpc.CmdRawGet:
		r := *req.RawGet()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchGet:
		r := *req.RawBatchGet()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawPut:
		r := *req.RawPut()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchPut:
		r := *req.RawBatchPut()
		r.Pairs = c.encodeParis(r.Pairs)
		newReq.Req = &r
	case tikvrpc.CmdRawDelete:
		r := *req.RawDelete()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchDelete:
		r := *req.RawBatchDelete()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawDeleteRange:
		r := *req.RawDeleteRange()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, false)
		newReq.Req = &r
	case tikvrpc.CmdRawScan:
		r := *req.RawScan()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, r.Reverse)
		newReq.Req = &r
	case tikvrpc.CmdGetKeyTTL:
		r := *req.RawGetKeyTTL()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawCompareAndSwap:
		r := *req.RawCompareAndSwap()
		r.Key = c.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawChecksum:
		r := *req.RawChecksum()
		r.Ranges = c.encodeKeyRanges(r.Ranges)
		newReq.Req = &r

	// TiFlash Requests
	case tikvrpc.CmdBatchCop:
		r := *req.BatchCop()
		r.Regions = c.encodeRegionInfos(r.Regions)
		r.TableRegions = c.encodeTableRegions(r.TableRegions)
		newReq.Req = &r
	case tikvrpc.CmdMPPTask:
		r := *req.DispatchMPPTask()
		r.Regions = c.encodeRegionInfos(r.Regions)
		r.TableRegions = c.encodeTableRegions(r.TableRegions)
		newReq.Req = &r

	// Other requests.
	case tikvrpc.CmdUnsafeDestroyRange:
		r := *req.UnsafeDestroyRange()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey, false)
		newReq.Req = &r
	case tikvrpc.CmdPhysicalScanLock:
		r := *req.PhysicalScanLock()
		r.StartKey = c.EncodeKey(r.StartKey)
		newReq.Req = &r
	case tikvrpc.CmdStoreSafeTS:
		r := *req.StoreSafeTS()
		r.KeyRange = c.encodeKeyRange(r.KeyRange)
		newReq.Req = &r
	case tikvrpc.CmdCop:
		r := *req.Cop()
		r.Ranges = c.encodeCopRanges(r.Ranges)
		newReq.Req = &r
	case tikvrpc.CmdCopStream:
		r := *req.Cop()
		r.Ranges = c.encodeCopRanges(r.Ranges)
		newReq.Req = &r
	case tikvrpc.CmdMvccGetByKey:
		r := *req.MvccGetByKey()
		r.Key = c.EncodeRegionKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdSplitRegion:
		r := *req.SplitRegion()
		r.SplitKeys = c.encodeKeys(r.SplitKeys)
		newReq.Req = &r
	}

	return &newReq, nil
}

// DecodeResponse decode the resp with the given codec.
func (c *codecV2) DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	var err error
	// Decode response based on command type.
	switch req.Type {
	// Transaction KV responses.
	// Keys that need to be decoded lies in RegionError, KeyError and LockInfo.
	case tikvrpc.CmdGet:
		r := resp.Resp.(*kvrpcpb.GetResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdScan:
		r := resp.Resp.(*kvrpcpb.ScanResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Pairs, err = c.decodePairs(r.Pairs)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdPrewrite:
		r := resp.Resp.(*kvrpcpb.PrewriteResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCommit:
		r := resp.Resp.(*kvrpcpb.CommitResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCleanup:
		r := resp.Resp.(*kvrpcpb.CleanupResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdBatchGet:
		r := resp.Resp.(*kvrpcpb.BatchGetResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Pairs, err = c.decodePairs(r.Pairs)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdBatchRollback:
		r := resp.Resp.(*kvrpcpb.BatchRollbackResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdScanLock:
		r := resp.Resp.(*kvrpcpb.ScanLockResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
		r.Locks, err = c.decodeLockInfos(r.Locks)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdResolveLock:
		r := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdGC:
		// TODO: Deprecate Central GC Mode.
		r := resp.Resp.(*kvrpcpb.GCResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdDeleteRange:
		r := resp.Resp.(*kvrpcpb.DeleteRangeResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdPessimisticLock:
		r := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdPessimisticRollback:
		r := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdTxnHeartBeat:
		r := resp.Resp.(*kvrpcpb.TxnHeartBeatResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCheckTxnStatus:
		r := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
		r.LockInfo, err = c.decodeLockInfo(r.LockInfo)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCheckSecondaryLocks:
		r := resp.Resp.(*kvrpcpb.CheckSecondaryLocksResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
		r.Locks, err = c.decodeLockInfos(r.Locks)
		if err != nil {
			return nil, err
		}
	// RawKV Responses.
	// Most of these responses does not require treatment aside from Region Error decoding.
	// Exceptions are Response with keys attach to them, like RawScan and RawBatchGet,
	// which need have their keys decoded.
	case tikvrpc.CmdRawGet:
		r := resp.Resp.(*kvrpcpb.RawGetResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Pairs, err = c.decodePairs(r.Pairs)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawPut:
		r := resp.Resp.(*kvrpcpb.RawPutResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawBatchPut:
		r := resp.Resp.(*kvrpcpb.RawBatchPutResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawDelete:
		r := resp.Resp.(*kvrpcpb.RawDeleteResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawBatchDelete:
		r := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawDeleteRange:
		r := resp.Resp.(*kvrpcpb.RawDeleteRangeResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Kvs, err = c.decodePairs(r.Kvs)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdGetKeyTTL:
		r := resp.Resp.(*kvrpcpb.RawGetKeyTTLResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawCompareAndSwap:
		r := resp.Resp.(*kvrpcpb.RawCASResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawChecksum:
		r := resp.Resp.(*kvrpcpb.RawChecksumResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}

	// Other requests.
	case tikvrpc.CmdUnsafeDestroyRange:
		r := resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdPhysicalScanLock:
		r := resp.Resp.(*kvrpcpb.PhysicalScanLockResponse)
		r.Locks, err = c.decodeLockInfos(r.Locks)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCop:
		r := resp.Resp.(*coprocessor.Response)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Locked, err = c.decodeLockInfo(r.Locked)
		if err != nil {
			return nil, err
		}
		r.Range, err = c.decodeCopRange(r.Range)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCopStream:
		return nil, errors.New("streaming coprocessor is not supported yet")
	case tikvrpc.CmdBatchCop, tikvrpc.CmdMPPTask:
		// There aren't range infos in BatchCop and MPPTask responses.
	case tikvrpc.CmdMvccGetByKey:
		r := resp.Resp.(*kvrpcpb.MvccGetByKeyResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdSplitRegion:
		r := resp.Resp.(*kvrpcpb.SplitRegionResponse)
		r.RegionError, err = c.decodeRegionError(r.RegionError)
		if err != nil {
			return nil, err
		}
		r.Regions, err = c.decodeRegions(r.Regions)
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func (c *codecV2) EncodeRegionKey(key []byte) []byte {
	encodeKey := c.EncodeKey(key)
	return c.memCodec.encodeKey(encodeKey)
}

func (c *codecV2) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	memDecoded, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.DecodeKey(memDecoded)
}

// EncodeRegionRange first append appropriate prefix to start and end,
// then pass them to memCodec to encode them to appropriate memory format.
func (c *codecV2) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedStart, encodedEnd := c.encodeRange(start, end, false)
	encodedStart = c.memCodec.encodeKey(encodedStart)
	encodedEnd = c.memCodec.encodeKey(encodedEnd)
	return encodedStart, encodedEnd
}

// DecodeRegionRange first decode key from memory compatible format,
// then pass decode them with DecodeRange to map them to correct range.
// Note that empty byte slice/ nil slice requires special treatment.
func (c *codecV2) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
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

	return c.DecodeRange(encodedStart, encodedEnd)
}

func (c *codecV2) EncodeRange(start, end []byte) ([]byte, []byte) {
	return c.encodeRange(start, end, false)
}

// encodeRange encodes start and end to correct range in APIv2.
// Note that if end is nil/ empty byte slice, it means no end.
// So we use endKey of the keyspace directly.
func (c *codecV2) encodeRange(start, end []byte, reverse bool) ([]byte, []byte) {
	// If reverse, scan from end to start.
	// Corresponding start and end encode needs to be reversed.
	if reverse {
		end, start = c.encodeRange(end, start, false)
		return start, end
	}
	var encodedEnd []byte
	if len(end) > 0 {
		encodedEnd = c.EncodeKey(end)
	} else {
		encodedEnd = c.endKey
	}
	return c.EncodeKey(start), encodedEnd
}

// DecodeRange maps encodedStart and end back to normal start and
// end without APIv2 prefixes.
func (c *codecV2) DecodeRange(encodedStart, encodedEnd []byte) (start []byte, end []byte, err error) {
	if bytes.Compare(encodedStart, c.endKey) >= 0 ||
		(len(encodedEnd) > 0 && bytes.Compare(encodedEnd, c.prefix) <= 0) {
		return nil, nil, errors.WithStack(errKeyOutOfBound)
	}

	start, end = []byte{}, []byte{}

	if bytes.HasPrefix(encodedStart, c.prefix) {
		start = encodedStart[len(c.prefix):]
	}

	if bytes.HasPrefix(encodedEnd, c.prefix) {
		end = encodedEnd[len(c.prefix):]
	}

	return
}

func (c *codecV2) EncodeKey(key []byte) []byte {
	return append(c.prefix, key...)
}

func (c *codecV2) DecodeKey(encodedKey []byte) ([]byte, error) {
	// If the given key does not start with the correct prefix,
	// return out of bound error.
	if !bytes.HasPrefix(encodedKey, c.prefix) {
		logutil.BgLogger().Warn("key not in keyspace",
			zap.String("keyspacePrefix", hex.EncodeToString(c.prefix)),
			zap.String("key", hex.EncodeToString(encodedKey)),
			zap.Stack("stack"))
		return nil, errKeyOutOfBound
	}
	return encodedKey[len(c.prefix):], nil
}

func (c *codecV2) encodeKeyRange(keyRange *kvrpcpb.KeyRange) *kvrpcpb.KeyRange {
	encodedRange := &kvrpcpb.KeyRange{}
	encodedRange.StartKey, encodedRange.EndKey = c.encodeRange(keyRange.StartKey, keyRange.EndKey, false)
	return encodedRange
}

func (c *codecV2) encodeKeyRanges(keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange {
	encodedRanges := make([]*kvrpcpb.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		encodedRanges = append(encodedRanges, c.encodeKeyRange(keyRange))
	}
	return encodedRanges
}

func (c *codecV2) encodeCopRange(r *coprocessor.KeyRange) *coprocessor.KeyRange {
	newRange := &coprocessor.KeyRange{}
	newRange.Start, newRange.End = c.encodeRange(r.Start, r.End, false)
	return newRange
}

func (c *codecV2) decodeCopRange(r *coprocessor.KeyRange) (*coprocessor.KeyRange, error) {
	var err error
	if r != nil {
		r.Start, r.End, err = c.DecodeRange(r.Start, r.End)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *codecV2) encodeCopRanges(ranges []*coprocessor.KeyRange) []*coprocessor.KeyRange {
	newRanges := make([]*coprocessor.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		newRanges = append(newRanges, c.encodeCopRange(r))
	}
	return newRanges
}

func (c *codecV2) decodeRegions(regions []*metapb.Region) ([]*metapb.Region, error) {
	var err error
	for _, region := range regions {
		region.StartKey, region.EndKey, err = c.DecodeRegionRange(region.StartKey, region.EndKey)
		if err != nil {
			return nil, err
		}
	}
	return regions, nil
}

func (c *codecV2) encodeKeys(keys [][]byte) [][]byte {
	var encodedKeys [][]byte
	for _, key := range keys {
		encodedKeys = append(encodedKeys, c.EncodeKey(key))
	}
	return encodedKeys
}

func (c *codecV2) encodeParis(pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	var encodedPairs []*kvrpcpb.KvPair
	for _, pair := range pairs {
		p := *pair
		p.Key = c.EncodeKey(p.Key)
		encodedPairs = append(encodedPairs, &p)
	}
	return encodedPairs
}

func (c *codecV2) decodePairs(encodedPairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	var pairs []*kvrpcpb.KvPair
	for _, encodedPair := range encodedPairs {
		var err error
		p := *encodedPair
		if p.Error != nil {
			p.Error, err = c.decodeKeyError(p.Error)
			if err != nil {
				return nil, err
			}
		}
		if len(p.Key) > 0 {
			p.Key, err = c.DecodeKey(p.Key)
			if err != nil {
				return nil, err
			}
		}
		pairs = append(pairs, &p)
	}
	return pairs, nil
}

func (c *codecV2) encodeMutations(mutations []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	var encodedMutations []*kvrpcpb.Mutation
	for _, mutation := range mutations {
		m := *mutation
		m.Key = c.EncodeKey(m.Key)
		encodedMutations = append(encodedMutations, &m)
	}
	return encodedMutations
}

func (c *codecV2) encodeRegionInfo(info *coprocessor.RegionInfo) *coprocessor.RegionInfo {
	i := *info
	i.Ranges = c.encodeCopRanges(info.Ranges)
	return &i
}

func (c *codecV2) encodeRegionInfos(infos []*coprocessor.RegionInfo) []*coprocessor.RegionInfo {
	var encodedInfos []*coprocessor.RegionInfo
	for _, info := range infos {
		encodedInfos = append(encodedInfos, c.encodeRegionInfo(info))
	}
	return encodedInfos
}

func (c *codecV2) encodeTableRegions(infos []*coprocessor.TableRegions) []*coprocessor.TableRegions {
	var encodedInfos []*coprocessor.TableRegions
	for _, info := range infos {
		i := *info
		i.Regions = c.encodeRegionInfos(info.Regions)
		encodedInfos = append(encodedInfos, &i)
	}
	return encodedInfos
}

func (c *codecV2) decodeRegionError(regionError *errorpb.Error) (*errorpb.Error, error) {
	if regionError == nil {
		return nil, nil
	}
	var err error
	if errInfo := regionError.KeyNotInRegion; errInfo != nil {
		errInfo.Key, err = c.DecodeKey(errInfo.Key)
		if err != nil {
			return nil, err
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
				// skip out of keyspace range's region
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

func (c *codecV2) decodeKeyError(keyError *kvrpcpb.KeyError) (*kvrpcpb.KeyError, error) {
	if keyError == nil {
		return nil, nil
	}
	var err error
	if keyError.Locked != nil {
		keyError.Locked, err = c.decodeLockInfo(keyError.Locked)
		if err != nil {
			return nil, err
		}
	}
	if keyError.Conflict != nil {
		keyError.Conflict.Key, err = c.DecodeKey(keyError.Conflict.Key)
		if err != nil {
			return nil, err
		}
		keyError.Conflict.Primary, err = c.DecodeKey(keyError.Conflict.Primary)
		if err != nil {
			return nil, err
		}
	}
	if keyError.AlreadyExist != nil {
		keyError.AlreadyExist.Key, err = c.DecodeKey(keyError.AlreadyExist.Key)
		if err != nil {
			return nil, err
		}
	}
	if keyError.Deadlock != nil {
		keyError.Deadlock.LockKey, err = c.DecodeKey(keyError.Deadlock.LockKey)
		if err != nil {
			return nil, err
		}
		for _, wait := range keyError.Deadlock.WaitChain {
			wait.Key, err = c.DecodeKey(wait.Key)
			if err != nil {
				return nil, err
			}
		}
	}
	if keyError.CommitTsExpired != nil {
		keyError.CommitTsExpired.Key, err = c.DecodeKey(keyError.CommitTsExpired.Key)
		if err != nil {
			return nil, err
		}
	}
	if keyError.TxnNotFound != nil {
		keyError.TxnNotFound.PrimaryKey, err = c.DecodeKey(keyError.TxnNotFound.PrimaryKey)
		if err != nil {
			return nil, err
		}
	}
	if keyError.AssertionFailed != nil {
		keyError.AssertionFailed.Key, err = c.DecodeKey(keyError.AssertionFailed.Key)
		if err != nil {
			return nil, err
		}
	}
	return keyError, nil
}

func (c *codecV2) decodeKeyErrors(keyErrors []*kvrpcpb.KeyError) ([]*kvrpcpb.KeyError, error) {
	var err error
	for i := range keyErrors {
		keyErrors[i], err = c.decodeKeyError(keyErrors[i])
		if err != nil {
			return nil, err
		}
	}
	return keyErrors, nil
}

func (c *codecV2) decodeLockInfo(info *kvrpcpb.LockInfo) (*kvrpcpb.LockInfo, error) {
	if info == nil {
		return nil, nil
	}
	var err error
	info.Key, err = c.DecodeKey(info.Key)
	if err != nil {
		return nil, err
	}
	info.PrimaryLock, err = c.DecodeKey(info.PrimaryLock)
	if err != nil {
		return nil, err
	}
	for i := range info.Secondaries {
		info.Secondaries[i], err = c.DecodeKey(info.Secondaries[i])
		if err != nil {
			return nil, err
		}
	}
	return info, nil
}

func (c *codecV2) decodeLockInfos(locks []*kvrpcpb.LockInfo) ([]*kvrpcpb.LockInfo, error) {
	var err error
	for i := range locks {
		locks[i], err = c.decodeLockInfo(locks[i])
		if err != nil {
			return nil, err
		}
	}
	return locks, nil
}
