package apicodec

import (
	"bytes"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

var (
	// APIV2RawKeyPrefix is prefix of raw key in API V2.
	APIV2RawKeyPrefix = []byte{'r', 0, 0, 0}

	// APIV2RawEndKey is max key of raw key in API V2.
	APIV2RawEndKey = []byte{'r', 0, 0, 1}

	// APIV2TxnKeyPrefix is prefix of txn key in API V2.
	APIV2TxnKeyPrefix = []byte{'x', 0, 0, 0}

	// APIV2TxnEndKey is max key of txn key in API V2.
	APIV2TxnEndKey = []byte{'x', 0, 0, 1}
)

// codecV2 is used to encode/decode keys and request into APIv2 format.
type codecV2 struct {
	prefix   []byte
	endKey   []byte
	mode     Mode
	memCodec memCodec
}

// NewCodecV2 returns a codec that can be used to encode/decode
// keys and requests to and from APIv2 format.
func NewCodecV2(mode Mode) Codec {
	// Region keys in CodecV2 are always encoded in memory comparable form.
	codec := &codecV2{
		mode:     mode,
		memCodec: &memComparableCodec{},
	}
	switch mode {
	case ModeRaw:
		codec.prefix = APIV2RawKeyPrefix
		codec.endKey = APIV2RawEndKey
	case ModeTxn:
		codec.prefix = APIV2TxnKeyPrefix
		codec.endKey = APIV2TxnEndKey
	default:
		panic("unknown mode")
	}
	return codec
}

func (c *codecV2) GetAPIVersion() kvrpcpb.APIVersion {
	return kvrpcpb.APIVersion_V2
}

func (c *codecV2) GetMode() Mode {
	return c.mode
}

func (c *codecV2) EncodeKey(key []byte) []byte {
	return append(c.prefix, key...)
}

func (c *codecV2) DecodeKey(encodedKey []byte) ([]byte, error) {
	if !bytes.HasPrefix(encodedKey, c.prefix) {
		return nil, errors.Errorf("invalid encoded key prefix: %q", encodedKey)
	}
	return encodedKey[len(c.prefix):], nil
}

// EncodeRange encodes start and end to correct range in APIv2.
// Note that if end is nil/ empty byte slice, it means no end.
// So we use endKey of the keyspace directly.
func (c *codecV2) EncodeRange(start, end []byte) ([]byte, []byte) {
	var encodedEnd []byte
	if len(end) > 0 {
		encodedEnd = c.EncodeKey(end)
	} else {
		encodedEnd = c.endKey
	}
	return c.EncodeKey(start), encodedEnd
}

// DecodeRange maps encodedStart and end back to normal start and
// end without APIv2 prefixes.
// Note that it uses empty byte slice to mark encoded start/end
// that lies outside the prefix's range.
func (c *codecV2) DecodeRange(encodedStart, encodedEnd []byte) ([]byte, []byte) {
	var start, end []byte
	if bytes.Compare(start, c.prefix) < 0 {
		start = []byte{}
	} else {
		start = encodedStart[len(c.prefix):]
	}
	if len(end) == 0 || bytes.Compare(end, c.endKey) >= 0 {
		end = []byte{}
	} else {
		end = encodedEnd[len(c.endKey):]
	}
	return start, end
}

func (c *codecV2) EncodeKeyRanges(keyRanges []*kvrpcpb.KeyRange) []*kvrpcpb.KeyRange {
	encodedRanges := make([]*kvrpcpb.KeyRange, 0, len(keyRanges))
	for i := 0; i < len(keyRanges); i++ {
		encodedRange := kvrpcpb.KeyRange{}
		encodedRange.StartKey, encodedRange.EndKey = c.EncodeRange(keyRanges[i].StartKey, keyRanges[i].EndKey)
		encodedRanges = append(encodedRanges, &encodedRange)
	}
	return encodedRanges
}

func (c *codecV2) EncodeKeys(keys [][]byte) [][]byte {
	var encodedKeys [][]byte
	for _, key := range keys {
		encodedKeys = append(encodedKeys, c.EncodeKey(key))
	}
	return encodedKeys
}

func (c *codecV2) EncodeParis(pairs []*kvrpcpb.KvPair) []*kvrpcpb.KvPair {
	var encodedPairs []*kvrpcpb.KvPair
	for _, pair := range pairs {
		p := *pair
		p.Key = c.EncodeKey(p.Key)
		encodedPairs = append(encodedPairs, &p)
	}
	return encodedPairs
}

func (c *codecV2) DecodePairs(encodedPairs []*kvrpcpb.KvPair) ([]*kvrpcpb.KvPair, error) {
	var pairs []*kvrpcpb.KvPair
	for _, encodedPair := range encodedPairs {
		var err error
		p := *encodedPair
		p.Key, err = c.DecodeKey(p.Key)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &p)
	}
	return pairs, nil
}

func (c *codecV2) EncodeRegionKey(key []byte) []byte {
	encodeKey := c.EncodeKey(key)
	return c.memCodec.encodeKey(encodeKey)
}

func (c *codecV2) DecodeRegionKey(encodedKey []byte) ([]byte, error) {
	memDecoded, err := c.memCodec.decodeKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return c.DecodeKey(memDecoded)
}

// EncodeRegionRange first append appropriate prefix to start and end,
// then pass them to memCodec to encode them to appropriate memory format.
func (c *codecV2) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	encodedStart, encodedEnd := c.EncodeRange(start, end)
	encodedStart = c.memCodec.encodeKey(encodedStart)
	encodedEnd = c.memCodec.encodeKey(encodedEnd)
	return encodedStart, encodedEnd
}

// DecodeRegionRange first decode key from memory compatible format,
// then pass decode them with DecodeRange to map them to correct range.
// Note that empty byte slice/ nil slice requires special treatment.
func (c *codecV2) DecodeRegionRange(encodedStart, encodedEnd []byte) ([]byte, []byte, error) {
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
	start, end := c.DecodeRange(encodedStart, encodedEnd)
	return start, end, nil
}
