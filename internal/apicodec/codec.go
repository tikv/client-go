package apicodec

import (
	"github.com/pingcap/errors"
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
	// GetKeyspace return the keyspace id of the codec.
	GetKeyspace() []byte
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
	DecodeRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error)
	// EncodeKey encode a key.
	EncodeKey(key []byte) []byte
	// DecodeKey decode a key.
	DecodeKey(encoded []byte) ([]byte, error)
}

func DecodeKey(encoded []byte, version kvrpcpb.APIVersion) ([]byte, []byte, error) {
	switch version {
	case kvrpcpb.APIVersion_V1:
		return nil, encoded, nil
	case kvrpcpb.APIVersion_V2:
		if len(encoded) < keyspacePrefixLen {
			return nil, nil, errors.Errorf("invalid V2 key: %s", encoded)
		}
		return encoded[:keyspacePrefixLen], encoded[keyspacePrefixLen:], nil
	}
	return nil, nil, errors.Errorf("unsupported api version %s", version.String())
}
