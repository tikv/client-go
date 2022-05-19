package client

import (
	"bytes"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
	. "github.com/tikv/client-go/v2/tikvrpc"
)

type Mode int

const (
	ModeRaw = iota
	ModeTxn
)

var (
	ApiV2RawKeyPrefix = []byte("r")
	ApiV2TxnKeyPrefix = []byte("x")
	ApiV2RawEndKey    = []byte{ApiV2RawKeyPrefix[0] + 1}
	ApiV2TxnEndKey    = []byte{ApiV2TxnKeyPrefix[0] + 1}
)

func GetV2Prefix(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return ApiV2RawKeyPrefix
	case ModeTxn:
		return ApiV2TxnKeyPrefix
	}
	panic("unreachable")
}

func GetV2EndKey(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return ApiV2RawEndKey
	case ModeTxn:
		return ApiV2TxnEndKey
	}
	panic("unreachable")
}

func EncodeV2Key(mode Mode, key []byte) []byte {
	return append(GetV2Prefix(mode), key...)
}

func EncodeV2Range(mode Mode, start, end []byte) ([]byte, []byte) {
	var b []byte
	if len(end) > 0 {
		b = EncodeV2Key(mode, end)
	} else {
		b = GetV2EndKey(mode)
	}
	return EncodeV2Key(mode, start), b
}

func MapV2RangeToV1(mode Mode, start []byte, end []byte) ([]byte, []byte) {
	var a, b []byte
	minKey := GetV2Prefix(mode)
	if bytes.Compare(start, minKey) < 0 {
		a = []byte{}
	} else {
		a = start[len(minKey):]
	}

	maxKey := GetV2EndKey(mode)
	if len(end) == 0 || bytes.Compare(end, maxKey) > 0 {
		b = []byte{}
	} else {
		b = end[len(maxKey):]
	}

	return a, b
}

func encodeV2Keys(mode Mode, keys [][]byte) [][]byte {
	for i, key := range keys {
		keys[i] = EncodeV2Key(mode, key)
	}
	return keys
}

func encodeV2Pairs(mode Mode, pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	for _, pair := range pairs {
		pair.Key = EncodeV2Key(mode, pair.Key)
	}
	return pairs
}

func EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return req, nil
	}

	if !req.ForRetry.CAS(false, true) {
		return req, nil
	}

	switch req.Type {
	case CmdGet:
	case CmdScan:
	case CmdPrewrite:
	case CmdPessimisticLock:
	case CmdPessimisticRollback:
	case CmdCommit:
	case CmdCleanup:
	case CmdBatchGet:
	case CmdBatchRollback:
	case CmdScanLock:
	case CmdResolveLock:
	case CmdGC:
	case CmdDeleteRange:
	case CmdRawGet:
		req.RawGet().Key = EncodeV2Key(ModeRaw, req.RawGet().Key)
	case CmdRawBatchGet:
		req.RawBatchGet().Keys = encodeV2Keys(ModeRaw, req.RawBatchGet().Keys)
	case CmdRawPut:
		req.RawPut().Key = EncodeV2Key(ModeRaw, req.RawPut().Key)
	case CmdRawBatchPut:
		req.RawBatchPut().Pairs = encodeV2Pairs(ModeRaw, req.RawBatchPut().Pairs)
	case CmdRawDelete:
		req.RawDelete().Key = EncodeV2Key(ModeRaw, req.RawDelete().Key)
	case CmdRawBatchDelete:
		req.RawBatchDelete().Keys = encodeV2Keys(ModeRaw, req.RawBatchDelete().Keys)
	case CmdRawDeleteRange:
		req.RawDeleteRange().StartKey, req.RawDeleteRange().EndKey =
			EncodeV2Range(ModeRaw, req.RawDeleteRange().StartKey, req.RawDeleteRange().EndKey)
	case CmdRawScan:
		req.RawScan().StartKey, req.RawScan().EndKey =
			EncodeV2Range(ModeRaw, req.RawScan().StartKey, req.RawScan().EndKey)
	case CmdGetKeyTTL:
		req.RawGetKeyTTL().Key = EncodeV2Key(ModeRaw, req.RawGetKeyTTL().Key)
	case CmdRawCompareAndSwap:
		req.RawCompareAndSwap().Key = EncodeV2Key(ModeRaw, req.RawCompareAndSwap().Key)
	case CmdUnsafeDestroyRange:
	case CmdRegisterLockObserver:
	case CmdCheckLockObserver:
	case CmdRemoveLockObserver:
	case CmdPhysicalScanLock:
	case CmdCop:
	case CmdCopStream:
	case CmdBatchCop:
	case CmdMPPTask:
	case CmdMvccGetByKey:
	case CmdMvccGetByStartTs:
	case CmdSplitRegion:
	case CmdEmpty:
	case CmdTxnHeartBeat:
	case CmdCheckTxnStatus:
	case CmdCheckSecondaryLocks:
	default:
		return nil, errors.Errorf("invalid request type %v", req.Type)
	}
	return req, nil
}

func decodeV2Key(mode Mode, key []byte) ([]byte, error) {
	prefix := GetV2Prefix(mode)
	if !bytes.HasPrefix(key, prefix) {
		return nil, errors.Errorf("invalid encoded key prefix: %q", key)
	}
	return key[len(prefix):], nil
}

func decodeV2Pairs(mode Mode, pairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	for _, pair := range pairs {
		var err error
		pair.Key, err = decodeV2Key(mode, pair.Key)
		if err != nil {
			return nil, err
		}
	}
	return pairs, nil
}

func DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return resp, nil
	}

	var err error

	switch req.Type {
	case CmdGet:
	case CmdScan:
	case CmdPrewrite:
	case CmdPessimisticLock:
	case CmdPessimisticRollback:
	case CmdCommit:
	case CmdCleanup:
	case CmdBatchGet:
	case CmdBatchRollback:
	case CmdScanLock:
	case CmdResolveLock:
	case CmdGC:
	case CmdDeleteRange:
	case CmdRawGet:
	case CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.Pairs, err = decodeV2Pairs(ModeRaw, r.Pairs)
	case CmdRawPut:
	case CmdRawBatchPut:
	case CmdRawDelete:
	case CmdRawBatchDelete:
	case CmdRawDeleteRange:
	case CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.Kvs, err = decodeV2Pairs(ModeRaw, r.Kvs)
	case CmdGetKeyTTL:
	case CmdRawCompareAndSwap:
	case CmdUnsafeDestroyRange:
	case CmdRegisterLockObserver:
	case CmdCheckLockObserver:
	case CmdRemoveLockObserver:
	case CmdPhysicalScanLock:
	case CmdCop:
	case CmdCopStream:
	case CmdBatchCop:
	case CmdMPPTask:
	case CmdMvccGetByKey:
	case CmdMvccGetByStartTs:
	case CmdSplitRegion:
	case CmdEmpty:
	case CmdTxnHeartBeat:
	case CmdCheckTxnStatus:
	case CmdCheckSecondaryLocks:
	default:
		return nil, errors.Errorf("invalid request type %v", req.Type)
	}

	if err != nil {
		return nil, err
	}

	return resp, nil
}
