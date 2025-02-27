/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package core

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesToHeader(t *testing.T) {
	// covered by record test
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

	bs := rh.Pack()
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
	assert.Equal(t, index, len(bs))

	// Deleted type
	rh = RecordHeader{
		uint32(0),
		Deleted,
		key.Size(),
		value.Size(),
	}

	bs = rh.Pack()
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
	assert.Equal(t, index, len(bs))
}
