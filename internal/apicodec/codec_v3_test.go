package apicodec

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestNewCodecV3(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{
		NamespaceId: 0x01020304,
		KeyspaceId:  0x050607,
	}
	codec, err := NewCodecV3(ModeRaw, identity, "ks")
	re.NoError(err)

	v3Codec := codec.(*codecV3)
	re.Equal(kvrpcpb.APIVersion_V3, v3Codec.GetAPIVersion())
	re.Equal([]byte{'r', 1, 2, 3, 4, 5, 6, 7}, v3Codec.physicalPrefix)
	re.Equal([]byte{'r', 1, 2, 3, 4, 5, 6, 8}, v3Codec.physicalEndKey)
	re.Equal(identity, v3Codec.GetKeyspaceMeta().GetKeyspaceIdentity())
	re.Equal(KeyspaceID(identity.KeyspaceId), v3Codec.GetKeyspaceID())
}

func TestNewCodecV3InvalidIdentity(t *testing.T) {
	re := require.New(t)

	_, err := NewCodecV3(ModeTxn, nil, "")
	re.Error(err)

	_, err = NewCodecV3(ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 0, KeyspaceId: 1}, "")
	re.Error(err)

	_, err = NewCodecV3(ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 1, KeyspaceId: 0}, "")
	re.Error(err)

	_, err = NewCodecV3(ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 1, KeyspaceId: maxKeyspaceID + 1}, "")
	re.Error(err)
}

func TestCodecV3EncodeRequestUsesLogicalKeys(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 9}
	codec, err := NewCodecV3(ModeTxn, identity, "ks")
	re.NoError(err)

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGet,
		Req: &kvrpcpb.GetRequest{
			Key: []byte("key"),
		},
	}
	encoded, err := codec.EncodeRequest(req)
	re.NoError(err)
	defer codec.(*codecV3).reqPool.Put(encoded)

	re.Equal([]byte("key"), encoded.Get().Key)
	re.Equal(kvrpcpb.APIVersion_V3, encoded.ApiVersion)
	re.Equal("ks", encoded.KeyspaceName)
	re.Equal(identity, encoded.GetKeyspaceIdentity())
}

func TestCodecV3EncodeRequestDoesNotEncodeScanBounds(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 9}
	codec, err := NewCodecV3(ModeTxn, identity, "ks")
	re.NoError(err)

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdScan,
		Req: &kvrpcpb.ScanRequest{
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}
	encoded, err := codec.EncodeRequest(req)
	re.NoError(err)
	defer codec.(*codecV3).reqPool.Put(encoded)

	re.Equal([]byte("a"), encoded.Scan().StartKey)
	re.Equal([]byte("b"), encoded.Scan().EndKey)
}

func TestCodecV3RegionKeysUseLogicalKeys(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{
		NamespaceId: 0x01020304,
		KeyspaceId:  0x050607,
	}
	codec, err := NewCodecV3(ModeTxn, identity, "ks")
	re.NoError(err)

	re.Equal([]byte("key"), codec.EncodeKey([]byte("key")))

	regionKey := codec.EncodeRegionKey([]byte("key"))
	logicalKey, err := codec.(*codecV3).memCodec.decodeKey(regionKey)
	re.NoError(err)
	re.Equal([]byte("key"), logicalKey)

	decodedKey, err := codec.DecodeRegionKey(regionKey)
	re.NoError(err)
	re.Equal([]byte("key"), decodedKey)
}

func TestCodecV3RegionRangeUsesLogicalKeys(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{
		NamespaceId: 0x01020304,
		KeyspaceId:  0x050607,
	}
	codec, err := NewCodecV3(ModeTxn, identity, "ks")
	re.NoError(err)
	v3Codec := codec.(*codecV3)

	encodedStart, encodedEnd := codec.EncodeRegionRange([]byte("a"), nil)

	logicalStart, err := v3Codec.memCodec.decodeKey(encodedStart)
	re.NoError(err)
	re.Equal([]byte("a"), logicalStart)
	re.Empty(encodedEnd)

	decodedStart, decodedEnd, err := codec.DecodeRegionRange(encodedStart, encodedEnd)
	re.NoError(err)
	re.Equal([]byte("a"), decodedStart)
	re.Empty(decodedEnd)
}
