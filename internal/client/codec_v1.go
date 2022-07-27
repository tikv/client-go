package client

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

type codecV1 struct {
	mode     Mode
	memCodec memCodec
}

func NewCodecV1(mode Mode) *codecV1 {
	switch mode {
	case ModeRaw:
		return &codecV1{
			mode:     mode,
			memCodec: &defaultMemCodec{},
		}
	case ModeTxn:
		return &codecV1{
			mode:     mode,
			memCodec: &memComparableCodec{},
		}
	}
	panic("unknown mode")
}

func (c *codecV1) GetAPIVersion() kvrpcpb.APIVersion {
	return kvrpcpb.APIVersion_V1
}

func (c *codecV1) GetMode() Mode {
	return c.mode
}

func (c *codecV1) EncodeKey(key []byte) []byte {
	return key
}

func (c *codecV1) DecodeKey(key []byte) ([]byte, error) {
	return key, nil
}

func (c *codecV1) EncodeRange(start, end []byte) ([]byte, []byte) {
	return start, end
}

func (c *codecV1) DecodeRange(start, end []byte) ([]byte, []byte) {
	return start, end
}

func (c *codecV1) EncodeKeyRanges(keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange {
	return keyRanges
}

func (c *codecV1) EncodeKeys(keys [][]byte) [][]byte {
	return keys
}

func (c *codecV1) EncodeParis(pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	return pairs
}

func (c *codecV1) DecodePairs(pairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	return pairs, nil
}

func (c *codecV1) EncodeRegionKey(key []byte) []byte {
	// In the context of region key, nil or empty slice has the special meaning of no bound,
	// so we skip encoding if given key is empty.
	if len(key) == 0 {
		return key
	}
	return c.memCodec.encodeKey(key)
}

func (c *codecV1) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	if len(encodedKey) == 0 {
		return encodedKey, nil
	}
	return c.memCodec.decodeKey(encodedKey)
}

func (c *codecV1) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return c.EncodeRegionKey(start), c.EncodeRegionKey(end)
}

func (c *codecV1) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
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
