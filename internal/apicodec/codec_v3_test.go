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
	re.Equal([]byte{'r', 1, 2, 3, 4, 5, 6, 7}, v3Codec.prefix)
	re.Equal([]byte{'r', 1, 2, 3, 4, 5, 6, 8}, v3Codec.endKey)
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

func TestCodecV3EncodeRequestKeepsUserKeys(t *testing.T) {
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

func TestCodecV3RegionKeysUsePhysicalPrefix(t *testing.T) {
	re := require.New(t)
	identity := &apipb.KeyspaceIdentity{
		NamespaceId: 0x01020304,
		KeyspaceId:  0x050607,
	}
	codec, err := NewCodecV3(ModeTxn, identity, "ks")
	re.NoError(err)

	physicalKey := codec.EncodeKey([]byte("key"))
	re.Equal([]byte{'x', 1, 2, 3, 4, 5, 6, 7, 'k', 'e', 'y'}, physicalKey)

	regionKey := codec.EncodeRegionKey([]byte("key"))
	decodedKey, err := codec.DecodeRegionKey(regionKey)
	re.NoError(err)
	re.Equal([]byte("key"), decodedKey)
}
