package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytes_Compare(t *testing.T) {
	bts1 := make(Bytes, 1)
	bts2 := make(Bytes, 1)
	bts1[0] = 1
	bts2[0] = 1
	assert.Equal(t, bts1, bts2)

	bts1[0] = 2
	assert.Equal(t, 1, bts1.Compare(bts2))

	bts1 = make(Bytes, 2)
	bts1[0] = 1
	bts1[1] = 2

	bts2 = make(Bytes, 2)
	bts2[0] = 1
	bts2[1] = 2
	assert.Equal(t, bts1, bts2)
	assert.Equal(t, 0, bts1.Compare(bts2))
	bts1[1] = 3
	assert.Equal(t, 1, bts1.Compare(bts2))
}

func TestBytes_Size(t *testing.T) {
	bts := make(Bytes, 1)
	assert.Equal(t, uint32(1), bts.Size())

	bts = make(Bytes, 100)
	assert.Equal(t, uint32(100), bts.Size())
}
