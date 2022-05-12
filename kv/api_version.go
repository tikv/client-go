package kv

import "bytes"

type Mode int

const (
	ModeRaw = iota
	ModeTxn
)

var (
	ApiV2RawKeyPrefix = []byte("r")
	ApiV2TxnKeyPrefix = []byte("x")
	ApiV2RawEndKey    = []byte{ApiV2RawKeyPrefix[0] + 1}
	ApiV2TxnEndKey    = []byte{ApiV2TxnKeyPrefix[0] + 1}
)

func GetV2Prefix(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return ApiV2RawKeyPrefix
	case ModeTxn:
		return ApiV2TxnKeyPrefix
	}
	panic("unreachable")
}

func GetV2EndKey(mode Mode) []byte {
	switch mode {
	case ModeRaw:
		return ApiV2RawEndKey
	case ModeTxn:
		return ApiV2TxnEndKey
	}
	panic("unreachable")
}

func BuildV2RequestKey(mode Mode, key []byte) []byte {
	return append(GetV2Prefix(mode), key...)
}

func DecodeV2StartKey(mode Mode, key []byte) []byte {
	minKey := GetV2Prefix(mode)
	if bytes.Compare(key, minKey) < 0 {
		return []byte{}
	}
	return key[len(minKey):]
}

func DecodeV2EndKey(mode Mode, key []byte) []byte {
	maxKey := GetV2EndKey(mode)
	if len(key) == 0 || bytes.Compare(key, maxKey) > 0 {
		return []byte{}
	}
	return key[len(maxKey):]
}
