package client

import (
	"bytes"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// Mode represents the operation mode of a request.
type Mode int

const (
	// ModeRaw represent a raw operation in TiKV
	ModeRaw = iota

	// ModeTxn represent a transaction operation in TiKV
	ModeTxn
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

func getV2Prefix(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return APIV2RawKeyPrefix
	case ModeTxn:
		return APIV2TxnKeyPrefix
	}
	panic("unreachable")
}

func getV2EndKey(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return APIV2RawEndKey
	case ModeTxn:
		return APIV2TxnEndKey
	}
	panic("unreachable")
}

// EncodeV2Key encode a user key into API V2 format.
func EncodeV2Key(mode Mode, key []byte) []byte {
	return append(getV2Prefix(mode), key...)
}

// EncodeV2Range encode a range into API V2 format.
func EncodeV2Range(mode Mode, start, end []byte) ([]byte, []byte) {
	var b []byte
	if len(end) > 0 {
		b = EncodeV2Key(mode, end)
	} else {
		b = getV2EndKey(mode)
	}
	return EncodeV2Key(mode, start), b
}

// EncodeV2KeyRanges encode KeyRange slice into API V2 formatted new slice.
func EncodeV2KeyRanges(mode Mode, keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange {
	encodedRanges := make([]*kvrpcpb.KeyRange, 0, len(keyRanges))
	for i := 0; i < len(keyRanges); i++ {
		keyRange := kvrpcpb.KeyRange{}
		keyRange.StartKey, keyRange.EndKey = EncodeV2Range(mode, keyRanges[i].StartKey, keyRanges[i].EndKey)
		encodedRanges = append(encodedRanges, &keyRange)
	}
	return encodedRanges
}

// MapV2RangeToV1 maps a range in API V2 format into V1 range.
// This function forbid the user seeing other keyspace.
func MapV2RangeToV1(mode Mode, start []byte, end []byte) ([]byte, []byte) {
	var a, b []byte
	minKey := getV2Prefix(mode)
	if bytes.Compare(start, minKey) < 0 {
		a = []byte{}
	} else {
		a = start[len(minKey):]
	}

	maxKey := getV2EndKey(mode)
	if len(end) == 0 || bytes.Compare(end, maxKey) >= 0 {
		b = []byte{}
	} else {
		b = end[len(maxKey):]
	}

	return a, b
}

// EncodeV2Keys encodes keys into API V2 format.
func EncodeV2Keys(mode Mode, keys [][]byte) [][]byte {
	var ks [][]byte
	for _, key := range keys {
		ks = append(ks, EncodeV2Key(mode, key))
	}
	return ks
}

// EncodeV2Pairs encodes pairs into API V2 format.
func EncodeV2Pairs(mode Mode, pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	var ps []*kvrpcpb.KvPair
	for _, pair := range pairs {
		p := *pair
		p.Key = EncodeV2Key(mode, p.Key)
		ps = append(ps, &p)
	}
	return ps
}

// EncodeRequest encodes req into specified API version format.
// NOTE: req is reused on retry. MUST encode on cloned request, other than overwrite the original.
func EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return req, nil
	}

	newReq := *req

	// TODO(iosmanthus): support transaction request types
	switch req.Type {
	case tikvrpc.CmdRawGet:
		r := *req.RawGet()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchGet:
		r := *req.RawBatchGet()
		r.Keys = EncodeV2Keys(ModeRaw, r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawPut:
		r := *req.RawPut()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchPut:
		r := *req.RawBatchPut()
		r.Pairs = EncodeV2Pairs(ModeRaw, r.Pairs)
		newReq.Req = &r
	case tikvrpc.CmdRawDelete:
		r := *req.RawDelete()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchDelete:
		r := *req.RawBatchDelete()
		r.Keys = EncodeV2Keys(ModeRaw, r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawDeleteRange:
		r := *req.RawDeleteRange()
		r.StartKey, r.EndKey = EncodeV2Range(ModeRaw, r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdRawScan:
		r := *req.RawScan()
		r.StartKey, r.EndKey = EncodeV2Range(ModeRaw, r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdGetKeyTTL:
		r := *req.RawGetKeyTTL()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawCompareAndSwap:
		r := *req.RawCompareAndSwap()
		r.Key = EncodeV2Key(ModeRaw, r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawChecksum:
		r := *req.RawChecksum()
		r.Ranges = EncodeV2KeyRanges(ModeRaw, r.Ranges)
		newReq.Req = &r
	}

	return &newReq, nil
}

// DecodeV2Key decodes API V2 encoded key into a normal user key.
func DecodeV2Key(mode Mode, key []byte) ([]byte, error) {
	prefix := getV2Prefix(mode)
	if !bytes.HasPrefix(key, prefix) {
		return nil, errors.Errorf("invalid encoded key prefix: %q", key)
	}
	return key[len(prefix):], nil
}

// DecodeV2Pairs decodes API V2 encoded pairs into normal user pairs.
func DecodeV2Pairs(mode Mode, pairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	var ps []*kvrpcpb.KvPair
	for _, pair := range pairs {
		var err error
		p := *pair
		p.Key, err = DecodeV2Key(mode, p.Key)
		if err != nil {
			return nil, err
		}
		ps = append(ps, &p)
	}
	return ps, nil
}

// DecodeResponse decode the resp in specified API version format.
func DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	if req.GetApiVersion() == kvrpcpb.APIVersion_V1 {
		return resp, nil
	}

	var err error

	switch req.Type {
	case tikvrpc.CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.Pairs, err = DecodeV2Pairs(ModeRaw, r.Pairs)
	case tikvrpc.CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.Kvs, err = DecodeV2Pairs(ModeRaw, r.Kvs)
	}

	return resp, err
}
