package apicodec

import (
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/util/codec"
)

// memCodec is used by Codec to encode/decode keys to
// memory comparable format.
type memCodec interface {
	encodeKey(key []byte) []byte
	decodeKey(encodedKey []byte) ([]byte, error)
}

// decodeError happens if the region range key is not well-formed.
// It indicates TiKV has bugs and the client can't handle such a case,
// so it should report the error to users soon.
type decodeError struct {
	error
}

// IsDecodeError is used to determine if error is decode error.
func IsDecodeError(err error) bool {
	_, ok := errors.Cause(err).(*decodeError)
	if !ok {
		_, ok = errors.Cause(err).(decodeError)
	}
	return ok
}

// defaultMemCodec is used by RawKV client under APIv1,
// It returns the key as given.
type defaultMemCodec struct{}

func (c *defaultMemCodec) encodeKey(key []byte) []byte {
	return key
}

func (c *defaultMemCodec) decodeKey(encodedKey []byte) ([]byte, error) {
	return encodedKey, nil
}

// memComparableCodec encode/decode key to/from mem comparable form.
// It throws decodeError on decode failure.
type memComparableCodec struct{}

func (c *memComparableCodec) encodeKey(key []byte) []byte {
	return codec.EncodeBytes([]byte(nil), key)
}

func (c *memComparableCodec) decodeKey(encodedKey []byte) ([]byte, error) {
	_, key, err := codec.DecodeBytes(encodedKey, nil)
	if err != nil {
		return nil, errors.WithStack(&decodeError{err})
	}
	return key, nil
}
