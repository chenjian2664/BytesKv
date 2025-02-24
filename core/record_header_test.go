package core

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToHeader(t *testing.T) {
}

func TestHeaderToBytes(t *testing.T) {
	testHeaderToBytes(t, Bytes("hello"), Bytes("world"))
	testHeaderToBytes(t, Bytes("你好"), Bytes("吃了吗"))
	testHeaderToBytes(t, Bytes("😂"), Bytes("😂xxx"))
}

func testHeaderToBytes(t *testing.T, key, value Bytes) {
	// normal type
	rh := RecordHeader{
		uint32(0),
		Normal,
		key.Size(),
		value.Size(),
	}

	bs := rh.pack()
	assert.NotNil(t, bs)

	index := 4
	assert.Equal(t, uint8(rh.Typ), bs[index])
	index += 1
	keySize, sz := binary.Varint(bs[index:])
	assert.Equal(t, int64(rh.KeySize), keySize)
	index += sz
	valueSize, sz := binary.Varint(bs[index:])
	assert.Equal(t, int64(rh.ValueSize), valueSize)
	index += sz

	// Deleted type
	rh = RecordHeader{
		uint32(0),
		Deleted,
		key.Size(),
		value.Size(),
	}

	bs = rh.pack()
	assert.NotNil(t, bs)

	index = 4
	assert.Equal(t, uint8(rh.Typ), bs[index])
	index += 1
	keySize, sz = binary.Varint(bs[index:])
	assert.Equal(t, int64(rh.KeySize), keySize)
	index += sz
	valueSize, sz = binary.Varint(bs[index:])
	assert.Equal(t, int64(rh.ValueSize), valueSize)
	index += sz
}
