package apicodec

import (
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type (
	// Mode represents the operation mode of a request.
	Mode int
	// KeyspaceID denotes the target keyspace of the request.
	KeyspaceID uint32
)

const (
	// ModeRaw represent a raw operation in TiKV.
	ModeRaw = iota
	// ModeTxn represent a transaction operation in TiKV.
	ModeTxn
)

const (
	// NullspaceID is a special keyspace id that represents no keyspace exist.
	NullspaceID KeyspaceID = 0xffffffff
)

// ParseKeyspaceID retrieves the keyspaceID from the given keyspace-encoded key.
// It returns error if the given key is not in proper api-v2 format.
func ParseKeyspaceID(b []byte) (KeyspaceID, error) {
	if err := checkV2Key(b); err != nil {
		return NullspaceID, err
	}

	buf := append([]byte{}, b[:keyspacePrefixLen]...)
	buf[0] = 0

	return KeyspaceID(binary.BigEndian.Uint32(buf)), nil
}

// Codec is responsible for encode/decode requests.
type Codec interface {
	// GetAPIVersion returns the api version of the codec.
	GetAPIVersion() kvrpcpb.APIVersion
	// GetKeyspace return the keyspace id of the codec in bytes.
	GetKeyspace() []byte
	// GetKeyspaceID return the keyspace id of the codec.
	GetKeyspaceID() KeyspaceID
	// EncodeRequest encodes with the given Codec.
	// NOTE: req is reused on retry. MUST encode on cloned request, other than overwrite the original.
	EncodeRequest(req *tikvrpc.Request) (*tikvrpc.Request, error)
	// DecodeResponse decode the resp with the given codec.
	DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error)
	// EncodeRegionKey encode region's key.
	EncodeRegionKey(key []byte) []byte
	// DecodeRegionKey decode region's key
	DecodeRegionKey(encodedKey []byte) ([]byte, error)
	// DecodeBucketKeys decode region bucket's key
	DecodeBucketKeys(keys [][]byte) ([][]byte, error)
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

// DecodeKey split a key to it's keyspace prefix and actual key.
func DecodeKey(encoded []byte, version kvrpcpb.APIVersion) ([]byte, []byte, error) {
	switch version {
	case kvrpcpb.APIVersion_V1:
		return nil, encoded, nil
	case kvrpcpb.APIVersion_V2:
		err := checkV2Key(encoded)
		if err != nil {
			return nil, nil, err
		}
		return encoded[:keyspacePrefixLen], encoded[keyspacePrefixLen:], nil
	}
	return nil, nil, errors.Errorf("unsupported api version %s", version.String())
}

func attachAPICtx(c Codec, req *tikvrpc.Request) *tikvrpc.Request {
	// Shallow copy the request to avoid concurrent modification.
	r := *req

	ctx := &r.Context
	ctx.ApiVersion = c.GetAPIVersion()
	ctx.KeyspaceId = uint32(c.GetKeyspaceID())

	switch r.Type {
	case tikvrpc.CmdMPPTask:
		mpp := *r.DispatchMPPTask()
		// Shallow copy the meta to avoid concurrent modification.
		meta := *mpp.Meta
		meta.KeyspaceId = ctx.KeyspaceId
		meta.ApiVersion = ctx.ApiVersion
		mpp.Meta = &meta
		r.Req = &mpp

	case tikvrpc.CmdCompact:
		compact := *r.Compact()
		compact.KeyspaceId = ctx.KeyspaceId
		compact.ApiVersion = ctx.ApiVersion
		r.Req = &compact
	}

	tikvrpc.AttachContext(&r, ctx)

	return &r
}
