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
	"hash/crc32"
	"testing"
)

func TestBytesToRecord(t *testing.T) {
	testBytesToRecord(t, Bytes{}, Bytes{}, Normal)
	testBytesToRecord(t, Bytes("hello"), Bytes("world"), Normal)
	testBytesToRecord(t, Bytes("hello"), Bytes("world"), Deleted)
	testBytesToRecord(t, Bytes("你好"), Bytes("吃了吗"), Normal)
	testBytesToRecord(t, Bytes("你好"), Bytes("吃了吗"), Deleted)
	testBytesToRecord(t, Bytes("😂"), Bytes("😂xxx"), Normal)
	testBytesToRecord(t, Bytes("😂"), Bytes("😂xxx"), Deleted)
}

func testBytesToRecord(t *testing.T, key, value Bytes, recordType RecordType) {
	record := &Record{
		key,
		value,
		recordType,
	}

	bts := record.pack()
	unpack := BytesToRecord(bts)
	assert.Equal(t, record.packHeader(), unpack.packHeader())
	assert.Equal(t, record, unpack)
}

func TestRecordToBytesCrc(t *testing.T) {
	key := Bytes("hello")
	value := Bytes("world")

	record := &Record{
		key,
		value,
		Normal,
	}

	header := record.packHeader()
	bs := record.pack()

	// [type, keySize, valueSize]
	assert.Equal(t, header[4:], bs[4:len(header)])

	// this crc is the whole record crc
	recordCrc := binary.LittleEndian.Uint32(bs[:4])
	assert.Equal(t, crc32.ChecksumIEEE(bs[4:]), recordCrc)

	headerCrc := binary.LittleEndian.Uint32(header[:4])
	// headerCrc not involve key,value, thus it shouldn't equal with record crc
	assert.NotEqual(t, headerCrc, recordCrc)

	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)

	assert.Equal(t, recordCrc, crc)
}
