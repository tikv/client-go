package apicodec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const apiV3KeyspacePrefixLen = keyspacePrefixLen

func checkV3Key(b []byte) error {
	if len(b) < apiV3KeyspacePrefixLen || (b[0] != RawModePrefix && b[0] != TxnModePrefix) {
		return errors.Errorf("invalid API V3 key %s", b)
	}
	return nil
}

// codecV3 uses API V3 request context for TiKV RPCs. TiKV RPC keys stay
// logical and are scoped by the request context, while PD region lookups use
// the physical keyspace range so the region cache cannot cross into another
// API V3 keyspace.
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
	keyspaceIDBytes, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}
	copy(physicalPrefix[1:], keyspaceIDBytes)

	physicalEndKey := make([]byte, apiV3KeyspacePrefixLen)
	prefixVal := binary.BigEndian.Uint32(physicalPrefix)
	binary.BigEndian.PutUint32(physicalEndKey, prefixVal+1)

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
	return c.memCodec.encodeKey(c.encodePhysicalKey(key))
}

func (c *codecV3) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	if len(encodedKey) == 0 {
		return encodedKey, nil
	}
	key, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.decodeRegionKey(key)
}

func (c *codecV3) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedEnd := c.physicalEndKey
	if len(end) > 0 {
		encodedEnd = c.encodePhysicalKey(end)
	}
	return c.memCodec.encodeKey(c.encodePhysicalKey(start)), c.memCodec.encodeKey(encodedEnd)
}

func (c *codecV3) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
	start, err := c.decodeRegionStart(encodedStart)
	if err != nil {
		return nil, nil, err
	}
	end, err := c.decodeRegionEnd(encodedEnd)
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

func (c *codecV3) encodePhysicalKey(key []byte) []byte {
	if bytes.HasPrefix(key, c.physicalPrefix) {
		return key
	}
	encoded := make([]byte, 0, len(c.physicalPrefix)+len(key))
	encoded = append(encoded, c.physicalPrefix...)
	encoded = append(encoded, key...)
	return encoded
}

func (c *codecV3) decodeRegionStart(encodedStart []byte) ([]byte, error) {
	if len(encodedStart) == 0 {
		return []byte{}, nil
	}
	start, err := c.memCodec.decodeKey(encodedStart)
	if err != nil {
		return nil, err
	}
	if isV3PhysicalKey(start) {
		if bytes.Compare(start, c.physicalEndKey) >= 0 {
			return nil, errors.WithStack(errKeyOutOfBound)
		}
		if bytes.Compare(start, c.physicalPrefix) < 0 {
			return []byte{}, nil
		}
	}
	return c.decodeRegionKey(start)
}

func (c *codecV3) decodeRegionEnd(encodedEnd []byte) ([]byte, error) {
	if len(encodedEnd) == 0 {
		return []byte{}, nil
	}
	end, err := c.memCodec.decodeKey(encodedEnd)
	if err != nil {
		return nil, err
	}
	if isV3PhysicalKey(end) {
		if bytes.Compare(end, c.physicalEndKey) >= 0 {
			return []byte{}, nil
		}
		if bytes.Compare(end, c.physicalPrefix) <= 0 {
			return nil, errors.WithStack(errKeyOutOfBound)
		}
	}
	return c.decodeRegionKey(end)
}

func (c *codecV3) decodeRegionKey(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return []byte{}, nil
	}
	if bytes.HasPrefix(key, c.physicalPrefix) {
		return key[len(c.physicalPrefix):], nil
	}
	if isV3PhysicalKey(key) {
		return nil, errors.WithStack(errKeyOutOfBound)
	}
	return key, nil
}

func isV3PhysicalKey(key []byte) bool {
	return len(key) >= apiV3KeyspacePrefixLen && (key[0] == RawModePrefix || key[0] == TxnModePrefix)
}
