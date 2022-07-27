package client

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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

type Codec interface {
	GetAPIVersion() kvrpcpb.APIVersion
	GetMode() Mode
	EncodeKey(key []byte) []byte
	EncodeKeys(keys [][]byte) [][]byte
	DecodeKey(encodedKey []byte) ([]byte, error)
	EncodeRange(start, end []byte) ([]byte, []byte)
	DecodeRange(encodedStart, encodedEnd []byte) ([]byte, []byte)
	EncodeKeyRanges(keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange
	EncodeParis(pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair
	DecodePairs(encodedPairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error)
	EncodeRegionKey(key []byte) []byte
	DecodeRegionKey(encodedKey []byte) ([]byte, error)
	EncodeRegionRange(start, end []byte) ([]byte, []byte)
	DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error)
}

// EncodeRequest encodes with the given Codec.
// NOTE: req is reused on retry. MUST encode on cloned request, other than overwrite the original.
func EncodeRequest(req *tikvrpc.Request, codec Codec) (*tikvrpc.Request, error) {
	newReq := *req
	// TODO: support transaction request types
	switch req.Type {
	case tikvrpc.CmdRawGet:
		r := *req.RawGet()
		r.Key = codec.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchGet:
		r := *req.RawBatchGet()
		r.Keys = codec.EncodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawPut:
		r := *req.RawPut()
		r.Key = codec.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchPut:
		r := *req.RawBatchPut()
		r.Pairs = codec.EncodeParis(r.Pairs)
		newReq.Req = &r
	case tikvrpc.CmdRawDelete:
		r := *req.RawDelete()
		r.Key = codec.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawBatchDelete:
		r := *req.RawBatchDelete()
		r.Keys = codec.EncodeKeys(r.Keys)
		newReq.Req = &r
	case tikvrpc.CmdRawDeleteRange:
		r := *req.RawDeleteRange()
		r.StartKey, r.EndKey = codec.EncodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdRawScan:
		r := *req.RawScan()
		r.StartKey, r.EndKey = codec.EncodeRange(r.StartKey, r.EndKey)
		newReq.Req = &r
	case tikvrpc.CmdGetKeyTTL:
		r := *req.RawGetKeyTTL()
		r.Key = codec.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawCompareAndSwap:
		r := *req.RawCompareAndSwap()
		r.Key = codec.EncodeKey(r.Key)
		newReq.Req = &r
	case tikvrpc.CmdRawChecksum:
		r := *req.RawChecksum()
		r.Ranges = codec.EncodeKeyRanges(r.Ranges)
		newReq.Req = &r
	}

	return &newReq, nil
}

// DecodeResponse decode the resp with the given codec.
func DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response, codec Codec) (*tikvrpc.Response, error) {
	var err error
	switch req.Type {
	case tikvrpc.CmdRawBatchGet:
		r := resp.Resp.(*kvrpcpb.RawBatchGetResponse)
		r.Pairs, err = codec.DecodePairs(r.Pairs)
	case tikvrpc.CmdRawScan:
		r := resp.Resp.(*kvrpcpb.RawScanResponse)
		r.Kvs, err = codec.DecodePairs(r.Kvs)
	}

	return resp, err
}
