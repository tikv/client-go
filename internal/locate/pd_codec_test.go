package locate

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/apicodec"
	pd "github.com/tikv/pd/client"
)

func TestDecodeRegionInPlace(t *testing.T) {
	prevC, err := apicodec.NewCodecV2(apicodec.ModeTxn, 1)
	require.Nil(t, err)
	c, err := apicodec.NewCodecV2(apicodec.ModeTxn, 2)
	require.Nil(t, err)
	nextC, err := apicodec.NewCodecV2(apicodec.ModeTxn, 3)
	require.Nil(t, err)
	cli := &CodecPDClient{
		Client: nil,
		codec:  c,
	}
	region0 := &pd.Region{
		Meta: &metapb.Region{
			StartKey: prevC.EncodeRegionKey([]byte("a")),
			EndKey:   nextC.EncodeRegionKey([]byte("b")),
		},
		Buckets: &metapb.Buckets{
			Keys: [][]byte{
				prevC.EncodeRegionKey([]byte("a")),
				prevC.EncodeRegionKey([]byte("b")),
				prevC.EncodeRegionKey([]byte("c")),
				c.EncodeRegionKey([]byte("")),
				c.EncodeRegionKey([]byte("b")),
				c.EncodeRegionKey([]byte("c")),
				nextC.EncodeRegionKey([]byte("")),
				nextC.EncodeRegionKey([]byte("b")),
			},
		},
	}
	err = cli.decodeRegionKeyInPlace(region0)
	require.Equal(t, 4, len(region0.Buckets.Keys))
	require.Empty(t, region0.Buckets.Keys[0])
	require.Equal(t, []byte("b"), region0.Buckets.Keys[1])
	require.Equal(t, []byte("c"), region0.Buckets.Keys[2])
	require.Empty(t, region0.Buckets.Keys[3])
	require.Nil(t, err)

	region1 := &pd.Region{
		Meta: &metapb.Region{
			StartKey: prevC.EncodeRegionKey([]byte("a")),
			EndKey:   nextC.EncodeRegionKey([]byte("b")),
		},
		Buckets: &metapb.Buckets{
			Keys: [][]byte{
				prevC.EncodeRegionKey([]byte("a")),
				prevC.EncodeRegionKey([]byte("b")),
				prevC.EncodeRegionKey([]byte("c")),
				c.EncodeRegionKey([]byte("b")),
				c.EncodeRegionKey([]byte("c")),
				nextC.EncodeRegionKey([]byte("")),
				nextC.EncodeRegionKey([]byte("b")),
			},
		},
	}
	err = cli.decodeRegionKeyInPlace(region1)
	require.Equal(t, 4, len(region1.Buckets.Keys))
	require.Empty(t, region1.Buckets.Keys[0])
	require.Equal(t, []byte("b"), region1.Buckets.Keys[1])
	require.Equal(t, []byte("c"), region1.Buckets.Keys[2])
	require.Empty(t, region1.Buckets.Keys[3])
	require.Nil(t, err)
}
