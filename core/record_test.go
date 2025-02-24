package core

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

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
