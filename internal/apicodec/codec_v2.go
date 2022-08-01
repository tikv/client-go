package apicodec

import (
	"bytes"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/tikv/client-go/v2/tikvrpc"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

var (
	// APIV2RawKeyPrefix is prefix of raw key in API V2.
	APIV2RawKeyPrefix = []byte{'r', 0, 0, 0}

	// APIV2RawEndKey is max key of raw key in API V2.
	APIV2RawEndKey = []byte{'r', 0, 0, 1}

	// APIV2TxnKeyPrefix is prefix of txn key in API V2.
	APIV2TxnKeyPrefix = []byte{'x', 0, 0, 0}

	// APIV2TxnEndKey is max key of txn key in API V2.
	APIV2TxnEndKey = []byte{'x', 0, 0, 1}
)

// codecV2 is used to encode/decode keys and request into APIv2 format.
type codecV2 struct {
	prefix   []byte
	endKey   []byte
	memCodec memCodec
}

// NewCodecV2 returns a codec that can be used to encode/decode
// keys and requests to and from APIv2 format.
func NewCodecV2(mode Mode) Codec {
	// Region keys in CodecV2 are always encoded in memory comparable form.
	codec := &codecV2{memCodec: &memComparableCodec{}}
	switch mode {
	case ModeRaw:
		codec.prefix = APIV2RawKeyPrefix
		codec.endKey = APIV2RawEndKey
	case ModeTxn:
		codec.prefix = APIV2TxnKeyPrefix
		codec.endKey = APIV2TxnEndKey
	default:
		panic("unknown mode")
	}
	return codec
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
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdScan:
		r := *req.Scan()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdPrewrite:
		r := *req.Prewrite()
		r.PrimaryLock = c.encodeKey(r.PrimaryLock)
		r.Secondaries = c.encodeKeys(r.Secondaries)
		newReq.Req = &r
	case tikvrpc.CmdCommit:
		r := *req.Commit()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdCleanup:
		r := *req.Cleanup()
		r.Key = c.encodeKey(r.Key)
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
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdResolveLock:
		r := *req.ResolveLock()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdGC:
		// TODO: Deprecate Central GC Mode.
	case tikvrpc.CmdDeleteRange:
		r := *req.DeleteRange()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdPessimisticLock:
		r := *req.PessimisticLock()
		r.PrimaryLock = c.encodeKey(r.PrimaryLock)
		for i := range r.Mutations {
			r.Mutations[i].Key = c.encodeKey(r.Mutations[i].Key)
		}
		newReq.Req = &r
	case tikvrpc.CmdPessimisticRollback:
		r := *req.PessimisticRollback()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdTxnHeartBeat:
		r := *req.TxnHeartBeat()
		r.PrimaryLock = c.encodeKey(r.PrimaryLock)
		newReq.Req = &r
	case tikvrpc.CmdCheckTxnStatus:
		r := *req.CheckTxnStatus()
		r.PrimaryKey = c.encodeKey(r.PrimaryKey)
		newReq.Req = &r
	case tikvrpc.CmdCheckSecondaryLocks:
		r := *req.CheckSecondaryLocks()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r

	// Raw Request Types.
	case tikvrpc.CmdRawGet:
		r := *req.RawGet()
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchGet:
		r := *req.RawBatchGet()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawPut:
		r := *req.RawPut()
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchPut:
		r := *req.RawBatchPut()
		r.Pairs = c.encodeParis(r.Pairs)
		newReq.Req = &r
	case tikvrpc.CmdRawDelete:
		r := *req.RawDelete()
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchDelete:
		r := *req.RawBatchDelete()
		r.Keys = c.encodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawDeleteRange:
		r := *req.RawDeleteRange()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdRawScan:
		r := *req.RawScan()
		r.StartKey, r.EndKey = c.encodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdGetKeyTTL:
		r := *req.RawGetKeyTTL()
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawCompareAndSwap:
		r := *req.RawCompareAndSwap()
		r.Key = c.encodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawChecksum:
		r := *req.RawChecksum()
		r.Ranges = c.encodeKeyRanges(r.Ranges)
		newReq.Req = &r
	}

	return &newReq, nil
}

// DecodeResponse decode the resp with the given codec.
func (c *codecV2) DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	regionError, err := resp.GetRegionError()
	if err != nil {
		return nil, err
	}
	decodedRegionError, err := c.decodeRegionError(regionError)
	if err != nil {
		return nil, err
	}
	// Decode response based on command type.
	switch req.Type {
	// Transaction KV responses.
	// Keys that need to be decoded lies in RegionError, KeyError and LockInfo.
	case tikvrpc.CmdGet:
		r := resp.Resp.(*kvrpcpb.GetResponse)
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdScan:
		r := resp.Resp.(*kvrpcpb.ScanResponse)
		r.RegionError = decodedRegionError
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
		r.RegionError = decodedRegionError
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCommit:
		r := resp.Resp.(*kvrpcpb.CommitResponse)
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCleanup:
		r := resp.Resp.(*kvrpcpb.CleanupResponse)
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdBatchGet:
		r := resp.Resp.(*kvrpcpb.BatchGetResponse)
		r.RegionError = decodedRegionError
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
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdScanLock:
		r := resp.Resp.(*kvrpcpb.ScanLockResponse)
		r.RegionError = decodedRegionError
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
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdGC:
		// TODO: Deprecate Central GC Mode.
		r := resp.Resp.(*kvrpcpb.GCResponse)
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdDeleteRange:
		r := resp.Resp.(*kvrpcpb.DeleteRangeResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdPessimisticLock:
		r := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
		r.RegionError = decodedRegionError
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdPessimisticRollback:
		r := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
		r.RegionError = decodedRegionError
		r.Errors, err = c.decodeKeyErrors(r.Errors)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdTxnHeartBeat:
		r := resp.Resp.(*kvrpcpb.TxnHeartBeatResponse)
		r.RegionError = decodedRegionError
		r.Error, err = c.decodeKeyError(r.Error)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdCheckTxnStatus:
		r := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		r.RegionError = decodedRegionError
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
		r.RegionError = decodedRegionError
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
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.RegionError = decodedRegionError
		r.Pairs, err = c.decodePairs(r.Pairs)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdRawPut:
		r := resp.Resp.(*kvrpcpb.RawPutResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawBatchPut:
		r := resp.Resp.(*kvrpcpb.RawBatchPutResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawDelete:
		r := resp.Resp.(*kvrpcpb.RawDeleteResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawBatchDelete:
		r := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawDeleteRange:
		r := resp.Resp.(*kvrpcpb.RawDeleteRangeResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.RegionError = decodedRegionError
		r.Kvs, err = c.decodePairs(r.Kvs)
		if err != nil {
			return nil, err
		}
	case tikvrpc.CmdGetKeyTTL:
		r := resp.Resp.(*kvrpcpb.RawGetKeyTTLResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawCompareAndSwap:
		r := resp.Resp.(*kvrpcpb.RawCASResponse)
		r.RegionError = decodedRegionError
	case tikvrpc.CmdRawChecksum:
		r := resp.Resp.(*kvrpcpb.RawChecksumResponse)
		r.RegionError = decodedRegionError
	}

	return resp, nil
}

func (c *codecV2) EncodeRegionKey(key []byte) []byte {
	encodeKey := c.encodeKey(key)
	return c.memCodec.encodeKey(encodeKey)
}

func (c *codecV2) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	memDecoded, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.decodeKey(memDecoded)
}

// EncodeRegionRange first append appropriate prefix to start and end,
// then pass them to memCodec to encode them to appropriate memory format.
func (c *codecV2) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedStart, encodedEnd := c.encodeRange(start, end)
	encodedStart = c.memCodec.encodeKey(encodedStart)
	encodedEnd = c.memCodec.encodeKey(encodedEnd)
	return encodedStart, encodedEnd
}

// DecodeRegionRange first decode key from memory compatible format,
// then pass decode them with decodeRange to map them to correct range.
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
	start, end := c.decodeRange(encodedStart, encodedEnd)
	return start, end, nil
}

func (c *codecV2) encodeKey(key []byte) []byte {
	return append(c.prefix, key...)
}

func (c *codecV2) decodeKey(encodedKey []byte) ([]byte, error) {
	if !bytes.HasPrefix(encodedKey, c.prefix) {
		return nil, errors.Errorf("invalid encoded key prefix: %q", encodedKey)
	}
	return encodedKey[len(c.prefix):], nil
}

// encodeRange encodes start and end to correct range in APIv2.
// Note that if end is nil/ empty byte slice, it means no end.
// So we use endKey of the keyspace directly.
func (c *codecV2) encodeRange(start, end []byte) ([]byte, []byte) {
	var encodedEnd []byte
	if len(end) > 0 {
		encodedEnd = c.encodeKey(end)
	} else {
		encodedEnd = c.endKey
	}
	return c.encodeKey(start), encodedEnd
}

// decodeRange maps encodedStart and end back to normal start and
// end without APIv2 prefixes.
// Note that it uses empty byte slice to mark encoded start/end
// that lies outside the prefix's range.
func (c *codecV2) decodeRange(encodedStart, encodedEnd []byte) ([]byte, []byte) {
	var start, end []byte
	if bytes.Compare(start, c.prefix) < 0 {
		start = []byte{}
	} else {
		start = encodedStart[len(c.prefix):]
	}
	if len(end) == 0 || bytes.Compare(end, c.endKey) >= 0 {
		end = []byte{}
	} else {
		end = encodedEnd[len(c.endKey):]
	}
	return start, end
}

func (c *codecV2) encodeKeyRanges(keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange {
	encodedRanges := make([]*kvrpcpb.KeyRange, 0, len(keyRanges))
	for i := 0; i < len(keyRanges); i++ {
		encodedRange := kvrpcpb.KeyRange{}
		encodedRange.StartKey, encodedRange.EndKey = c.encodeRange(keyRanges[i].StartKey, keyRanges[i].EndKey)
		encodedRanges = append(encodedRanges, &encodedRange)
	}
	return encodedRanges
}

func (c *codecV2) encodeKeys(keys [][]byte) [][]byte {
	var encodedKeys [][]byte
	for _, key := range keys {
		encodedKeys = append(encodedKeys, c.encodeKey(key))
	}
	return encodedKeys
}

func (c *codecV2) encodeParis(pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	var encodedPairs []*kvrpcpb.KvPair
	for _, pair := range pairs {
		p := *pair
		p.Key = c.encodeKey(p.Key)
		encodedPairs = append(encodedPairs, &p)
	}
	return encodedPairs
}

func (c *codecV2) decodePairs(encodedPairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	var pairs []*kvrpcpb.KvPair
	for _, encodedPair := range encodedPairs {
		var err error
		p := *encodedPair
		p.Key, err = c.decodeKey(p.Key)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &p)
	}
	return pairs, nil
}

func (c *codecV2) decodeRegionError(regionError *errorpb.Error) (*errorpb.Error, error) {
	var err error
	if errInfo := regionError.KeyNotInRegion; errInfo != nil {
		errInfo.Key, err = c.decodeKey(errInfo.Key)
		if err != nil {
			return nil, err
		}
		errInfo.StartKey, errInfo.EndKey, err = c.DecodeRegionRange(errInfo.StartKey, errInfo.EndKey)
	}

	if errInfo := regionError.EpochNotMatch; errInfo != nil {
		for _, meta := range errInfo.CurrentRegions {
			meta.StartKey, meta.EndKey, err = c.DecodeRegionRange(meta.StartKey, meta.EndKey)
			if err != nil {
				return nil, err
			}
		}
	}

	return regionError, nil
}

func (c *codecV2) decodeKeyError(keyError *kvrpcpb.KeyError) (*kvrpcpb.KeyError, error) {
	var err error
	if keyError.Locked != nil {
		keyError.Locked, err = c.decodeLockInfo(keyError.Locked)
		if err != nil {
			return nil, err
		}
	}
	if keyError.Conflict != nil {
		keyError.Conflict.Key, err = c.decodeKey(keyError.Conflict.Key)
		if err != nil {
			return nil, err
		}
		keyError.Conflict.Primary, err = c.decodeKey(keyError.Conflict.Primary)
		if err != nil {
			return nil, err
		}
	}
	if keyError.AlreadyExist != nil {
		keyError.AlreadyExist.Key, err = c.decodeKey(keyError.AlreadyExist.Key)
		if err != nil {
			return nil, err
		}
	}
	if keyError.Deadlock != nil {
		keyError.Deadlock.LockKey, err = c.decodeKey(keyError.Deadlock.LockKey)
		if err != nil {
			return nil, err
		}
		for _, wait := range keyError.Deadlock.WaitChain {
			wait.Key, err = c.decodeKey(wait.Key)
			if err != nil {
				return nil, err
			}
		}
	}
	if keyError.CommitTsExpired != nil {
		keyError.CommitTsExpired.Key, err = c.decodeKey(keyError.CommitTsExpired.Key)
		if err != nil {
			return nil, err
		}
	}
	if keyError.TxnNotFound != nil {
		keyError.TxnNotFound.PrimaryKey, err = c.decodeKey(keyError.TxnNotFound.PrimaryKey)
		if err != nil {
			return nil, err
		}
	}
	if keyError.AssertionFailed != nil {
		keyError.AssertionFailed.Key, err = c.decodeKey(keyError.AssertionFailed.Key)
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
	var err error
	info.Key, err = c.decodeKey(info.Key)
	if err != nil {
		return nil, err
	}
	info.PrimaryLock, err = c.decodeKey(info.PrimaryLock)
	if err != nil {
		return nil, err
	}
	for i := range info.Secondaries {
		info.Secondaries[i], err = c.decodeKey(info.Secondaries[i])
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
