package apicodec

import (
	"encoding/binary"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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

// codecV3 uses API V3 request context for TiKV RPCs. PD region lookups also
// use logical keys because the wrapped PD client is already scoped by
// KeyspaceIdentity.
type codecV3 struct {
	*codecV2
	physicalPrefix []byte
	physicalEndKey []byte
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

	physicalPrefix := make([]byte, apiV3KeyspacePrefixLen)
	switch mode {
	case ModeRaw:
		physicalPrefix[0] = RawModePrefix
	case ModeTxn:
		physicalPrefix[0] = TxnModePrefix
	default:
		return nil, errors.Errorf("unknown mode")
	}
	binary.BigEndian.PutUint32(physicalPrefix[1:5], namespaceID)
	keyspaceIDBytes, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}
	copy(physicalPrefix[5:], keyspaceIDBytes)

	physicalEndKey := make([]byte, apiV3KeyspacePrefixLen)
	prefixVal := binary.BigEndian.Uint64(physicalPrefix)
	binary.BigEndian.PutUint64(physicalEndKey, prefixVal+1)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Name:             BuildKeyspaceName(keyspaceName),
		KeyspaceIdentity: identity,
	}
	base := &codecV2{
		apiVersion:   kvrpcpb.APIVersion_V3,
		memCodec:     &memComparableCodec{},
		keyspaceMeta: keyspaceMeta,
	}
	base.reqPool.New = func() any { return &tikvrpc.Request{} }
	return &codecV3{codecV2: base, physicalPrefix: physicalPrefix, physicalEndKey: physicalEndKey}, nil
}

func (c *codecV3) DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	return c.codecV2.DecodeResponse(req, resp)
}

func (c *codecV3) GetKeyspace() []byte {
	return c.physicalPrefix
}

func (c *codecV3) EncodeRegionKey(key []byte) []byte {
	return c.memCodec.encodeKey(key)
}

func (c *codecV3) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	if len(encodedKey) == 0 {
		return encodedKey, nil
	}
	return c.memCodec.decodeKey(encodedKey)
}

func (c *codecV3) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	if len(end) > 0 {
		return c.EncodeRegionKey(start), c.EncodeRegionKey(end)
	}
	return c.EncodeRegionKey(start), end
}

func (c *codecV3) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
	start, err := c.DecodeRegionKey(encodedStart)
	if err != nil {
		return nil, nil, err
	}
	end, err := c.DecodeRegionKey(encodedEnd)
	if err != nil {
		return nil, nil, err
	}
	return start, end, nil
}

func (c *codecV3) DecodeBucketKeys(keys [][]byte) ([][]byte, error) {
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		k, err := c.DecodeRegionKey(key)
		if err != nil {
			return nil, err
		}
		ks = append(ks, k)
	}
	return ks, nil
}
