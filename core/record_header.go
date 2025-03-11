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
)

// crc type keySize valueSize
// 4    1    5 			5		= 15
const MaxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

// RecordHeader the header of the record
type RecordHeader struct {
	Crc       uint32
	Typ       RecordType
	KeySize   uint32
	ValueSize uint32
}

func (rh *RecordHeader) Pack() Bytes {
	header := make(Bytes, MaxLogRecordHeaderSize)

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

func BytesToHeader(bs Bytes) (*RecordHeader, int) {
	crc := binary.LittleEndian.Uint32(bs[:4])
	typ := RecordType(bs[4])

	index := 5
	keySize, n := binary.Varint(bs[index:])
	index += n
	valueSize, n := binary.Varint(bs[index:])
	index += n

	return &RecordHeader{
		Crc:       crc,
		Typ:       typ,
		KeySize:   uint32(keySize),
		ValueSize: uint32(valueSize),
	}, index
}
