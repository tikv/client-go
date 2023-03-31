package apicodec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/util/codec"
)

func TestV1DecodeBucketKey(t *testing.T) {
	c := NewCodecV1(ModeTxn)
	raw := []byte("test")
	encoded := codec.EncodeBytes(nil, raw)
	key, err := c.DecodeBucketKey(encoded)
	assert.Nil(t, err)
	assert.Equal(t, raw, key)

	raw = []byte{}
	encoded = codec.EncodeBytes(nil, raw)
	key, err = c.DecodeBucketKey(encoded)
	assert.Nil(t, err)
	assert.Equal(t, raw, key)
}
