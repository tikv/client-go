package apicodec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseKeyspaceID(t *testing.T) {
	id, err := ParseKeyspaceID([]byte{'x', 1, 2, 3, 1, 2, 3})
	assert.Nil(t, err)
	assert.Equal(t, KeyspaceID(0x010203), id)

	id, err = ParseKeyspaceID([]byte{'r', 1, 2, 3, 1, 2, 3, 4})
	assert.Nil(t, err)
	assert.Equal(t, KeyspaceID(0x010203), id)

	id, err = ParseKeyspaceID([]byte{'t', 0, 0})
	assert.NotNil(t, err)
	assert.Equal(t, NulSpaceID, id)

	id, err = ParseKeyspaceID([]byte{'t', 0, 0, 1, 1, 2, 3})
	assert.NotNil(t, err)
	assert.Equal(t, NulSpaceID, id)
}
