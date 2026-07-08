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

const apiV3KeyspacePrefixLen = 8

func checkV3Key(b []byte) error {
	if len(b) < apiV3KeyspacePrefixLen || (b[0] != RawModePrefix && b[0] != TxnModePrefix) {
		return errors.Errorf("invalid API V3 key %s", b)
	}
	return nil
}

// codecV3 uses API V3 request context for TiKV RPCs and API V3 physical
// prefixes for PD region lookups.
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
	return c.memCodec.encodeKey(c.encodePhysicalKey(key))
}

func (c *codecV3) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	physicalKey, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.decodePhysicalKey(physicalKey)
}

func (c *codecV3) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedStart := c.memCodec.encodeKey(c.encodePhysicalKey(start))
	encodedEnd := c.physicalEndKey
	if len(end) > 0 {
		encodedEnd = c.encodePhysicalKey(end)
	}
	encodedEnd = c.memCodec.encodeKey(encodedEnd)
	return encodedStart, encodedEnd
}

func (c *codecV3) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
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
	return c.decodePhysicalRange(encodedStart, encodedEnd)
}

func (c *codecV3) DecodeBucketKeys(keys [][]byte) ([][]byte, error) {
	ks := make([][]byte, 0, len(keys))
	for i, key := range keys {
		var (
			k   []byte
			err error
		)
		if len(key) > 0 {
			k, err = c.memCodec.decodeKey(key)
		}
		if err != nil {
			return nil, err
		}

		if i == 0 && bytes.Compare(k, c.physicalPrefix) < 0 {
			ks = append(ks, []byte{})
		} else if i == len(keys)-1 && (len(k) == 0 || bytes.Compare(k, c.physicalEndKey) >= 0) {
			ks = append(ks, []byte{})
		} else if bytes.HasPrefix(k, c.physicalPrefix) {
			raw := k[len(c.physicalPrefix):]
			if len(raw) == 0 && len(ks) > 0 && len(ks[0]) == 0 {
				continue
			}
			ks = append(ks, raw)
		}
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

func (c *codecV3) decodePhysicalRange(encodedStart, encodedEnd []byte) (start []byte, end []byte, err error) {
	if bytes.Compare(encodedStart, c.physicalEndKey) >= 0 ||
		(len(encodedEnd) > 0 && bytes.Compare(encodedEnd, c.physicalPrefix) <= 0) {
		return nil, nil, errors.WithStack(errKeyOutOfBound)
	}

	start, end = []byte{}, []byte{}
	if bytes.HasPrefix(encodedStart, c.physicalPrefix) {
		start = encodedStart[len(c.physicalPrefix):]
	}
	if bytes.HasPrefix(encodedEnd, c.physicalPrefix) {
		end = encodedEnd[len(c.physicalPrefix):]
	}
	return start, end, nil
}

func (c *codecV3) decodePhysicalKey(encodedKey []byte) ([]byte, error) {
	if len(encodedKey) == 0 {
		return nil, nil
	}
	if !bytes.HasPrefix(encodedKey, c.physicalPrefix) {
		return nil, errKeyOutOfBound
	}
	return encodedKey[len(c.physicalPrefix):], nil
}
