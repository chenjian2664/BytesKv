package core

import "encoding/binary"

// crc type keySize valueSize
// 4    1    5 			5		= 15
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

// RecordHeader the header of the record
type RecordHeader struct {
	Crc       uint32
	Typ       RecordType
	KeySize   uint32
	ValueSize uint32
}

func (rh *RecordHeader) pack() Bytes {
	header := make(Bytes, maxLogRecordHeaderSize)

	// crc
	binary.LittleEndian.PutUint32(header[:4], rh.Crc)
	// type
	header[4] = byte(rh.Typ)

	index := uint32(5)
	// keySize
	index += uint32(binary.PutVarint(header[index:], int64(rh.KeySize)))
	// valueSize
	index += uint32(binary.PutVarint(header[index:], int64(rh.ValueSize)))
	return header[:index]
}

func BytesToHeader(b Bytes) *RecordHeader {
	return nil
}
