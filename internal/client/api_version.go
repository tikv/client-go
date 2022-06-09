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
	ApiV2RawKeyPrefix = []byte{'r', 0, 0, 0}
	ApiV2RawEndKey    = []byte{'r', 0, 0, 1}

	ApiV2TxnKeyPrefix = []byte{'x', 0, 0, 0}
	ApiV2TxnEndKey    = []byte{'x', 0, 0, 1}
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
	if len(end) == 0 || bytes.Compare(end, maxKey) >= 0 {
		b = []byte{}
	} else {
		b = end[len(maxKey):]
	}

	return a, b
}

func EncodeV2Keys(mode Mode, keys [][]byte) [][]byte {
	var ks [][]byte
	for _, key := range keys {
		ks = append(ks, EncodeV2Key(mode, key))
	}
	return ks
}

func EncodeV2Pairs(mode Mode, pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	var ps []*kvrpcpb.KvPair
	for _, pair := range pairs {
		p := *pair
		p.Key = EncodeV2Key(mode, p.Key)
		ps = append(ps, &p)
	}
	return ps
}

func EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return req, nil
	}

	newReq := *req

	// TODO(iosmanthus): support transaction request types
	switch req.Type {
	case CmdRawGet:
		r := *req.RawGet()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case CmdRawBatchGet:
		r := *req.RawBatchGet()
		r.Keys = EncodeV2Keys(ModeRaw, r.Keys)
		newReq.Req = &r
	case CmdRawPut:
		r := *req.RawPut()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case CmdRawBatchPut:
		r := *req.RawBatchPut()
		r.Pairs = EncodeV2Pairs(ModeRaw, r.Pairs)
		newReq.Req = &r
	case CmdRawDelete:
		r := *req.RawDelete()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case CmdRawBatchDelete:
		r := *req.RawBatchDelete()
		r.Keys = EncodeV2Keys(ModeRaw, r.Keys)
		newReq.Req = &r
	case CmdRawDeleteRange:
		r := *req.RawDeleteRange()
		r.StartKey, r.EndKey = EncodeV2Range(ModeRaw, r.StartKey, r.EndKey)
		newReq.Req = &r
	case CmdRawScan:
		r := *req.RawScan()
		r.StartKey, r.EndKey = EncodeV2Range(ModeRaw, r.StartKey, r.EndKey)
		newReq.Req = &r
	case CmdGetKeyTTL:
		r := *req.RawGetKeyTTL()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case CmdRawCompareAndSwap:
		r := *req.RawCompareAndSwap()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	}

	return &newReq, nil
}

func DecodeKey(mode Mode, key []byte) ([]byte, error) {
	prefix := GetV2Prefix(mode)
	if !bytes.HasPrefix(key, prefix) {
		return nil, errors.Errorf("invalid encoded key prefix: %q", key)
	}
	return key[len(prefix):], nil
}

func DecodePairs(mode Mode, pairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	var ps []*kvrpcpb.KvPair
	for _, pair := range pairs {
		var err error
		p := *pair
		p.Key, err = DecodeKey(mode, p.Key)
		if err != nil {
			return nil, err
		}
		ps = append(ps, &p)
	}
	return ps, nil
}

func DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return resp, nil
	}

	var err error

	switch req.Type {
	case CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.Pairs, err = DecodePairs(ModeRaw, r.Pairs)
	case CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.Kvs, err = DecodePairs(ModeRaw, r.Kvs)
	}

	return resp, err
}
