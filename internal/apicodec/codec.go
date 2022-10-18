package apicodec

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

// Codec is responsible for encode/decode requests.
type Codec interface {
	// GetAPIVersion returns the api version of the codec.
	GetAPIVersion() kvrpcpb.APIVersion
	// GetKeyspaceID return the keyspace id of the codec.
	GetKeyspaceID() []byte
	// EncodeRequest encodes with the given Codec.
	// NOTE: req is reused on retry. MUST encode on cloned request, other than overwrite the original.
	EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error)
	// DecodeResponse decode the resp with the given codec.
	DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error)
	// EncodeRegionKey encode region's key.
	EncodeRegionKey(key []byte) []byte
	// DecodeRegionKey decode region's key
	DecodeRegionKey(encodedKey []byte) ([]byte, error)
	// EncodeRegionRange encode region's start and end.
	EncodeRegionRange(start, end []byte) ([]byte, []byte)
	// DecodeRegionRange decode region's start and end.
	DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error)
	// EncodeRange encode a key range.
	EncodeRange(start, end []byte) ([]byte, []byte)
	// DecodeRange decode a key range.
	DecodeRange(start, end []byte) ([]byte, []byte)
	// EncodeKey encode a key.
	EncodeKey(key []byte) []byte
}
