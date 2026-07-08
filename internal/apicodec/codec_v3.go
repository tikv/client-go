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

// codecV3 uses API V3 request context for TiKV RPCs and API V3 physical
// prefixes for PD region lookups.
type codecV3 struct {
	*codecV2
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

	prefix := make([]byte, apiV3KeyspacePrefixLen)
	switch mode {
	case ModeRaw:
		prefix[0] = RawModePrefix
	case ModeTxn:
		prefix[0] = TxnModePrefix
	default:
		return nil, errors.Errorf("unknown mode")
	}
	binary.BigEndian.PutUint32(prefix[1:5], namespaceID)
	keyspaceIDBytes, err := getIDByte(keyspaceID)
	if err != nil {
		return nil, err
	}
	copy(prefix[5:], keyspaceIDBytes)

	endKey := make([]byte, apiV3KeyspacePrefixLen)
	prefixVal := binary.BigEndian.Uint64(prefix)
	binary.BigEndian.PutUint64(endKey, prefixVal+1)

	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Name:             BuildKeyspaceName(keyspaceName),
		KeyspaceIdentity: identity,
	}
	base := &codecV2{
		apiVersion:   kvrpcpb.APIVersion_V3,
		prefix:       prefix,
		endKey:       endKey,
		memCodec:     &memComparableCodec{},
		keyspaceMeta: keyspaceMeta,
	}
	base.reqPool.New = func() any { return &tikvrpc.Request{} }
	return &codecV3{codecV2: base}, nil
}

func (c *codecV3) DecodeResponse(req *tikvrpc.Request, resp *tikvrpc.Response) (*tikvrpc.Response, error) {
	return c.codecV2.DecodeResponse(req, resp)
}
