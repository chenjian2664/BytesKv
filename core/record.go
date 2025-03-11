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
	"hash/crc32"
	"strconv"
)

type RecordType byte

const (
	// Normal normal append data
	Normal RecordType = iota
	// Deleted the record is deleted
	Deleted
)

// Record the record that use between index and storage
// record:
//  1. header: crc, type, keySize, valueSize
//  2. data: key, value
type Record struct {
	Key   Bytes
	Value Bytes
	Type  RecordType
}

func (r *Record) PackHeader() Bytes {
	header := make(Bytes, MaxLogRecordHeaderSize)
	// type
	header[4] = byte(r.Type)

	// Write keySize
	var index = 5
	index += binary.PutVarint(header[index:], int64(r.Key.Size()))
	// Write valueSize
	index += binary.PutVarint(header[index:], int64(r.Value.Size()))

	// Write crc
	crc := crc32.ChecksumIEEE(header[4:])
	// TODO: support BigEndian as well
	binary.LittleEndian.PutUint32(header[:4], crc)

	return header[:index]
}

func (r *Record) Pack() Bytes {
	header := r.PackHeader()
	record := make(Bytes, header.Size()+r.Key.Size()+r.Value.Size())
	copy(record, header)

	if Bytes(record[:header.Size()]).Compare(header) != 0 {
		panic("Error data copy")
	}

	var index = header.Size()
	// copy key data
	index += uint32(copy(record[index:], r.Key))
	// copy value data
	index += uint32(copy(record[index:], r.Value))

	// shouldn't
	if index != record.Size() {
		panic("Error data copying")
	}

	crc := crc32.ChecksumIEEE(record[4:])
	binary.LittleEndian.PutUint32(record[:4], crc)
	return record
}

func BytesToRecord(bts Bytes) *Record {
	header, index := BytesToHeader(bts)
	// add crc check by default
	if header.Crc != crc32.ChecksumIEEE(bts[4:]) {
		panic("Corrupted data, crc not match")
	}

	key := bts[index : index+int(header.KeySize)]
	index += int(header.KeySize)

	// TODO: add error handling
	if index+int(header.ValueSize) != int(bts.Size()) {
		panic("Corrupted data:" + strconv.Itoa(index) + "," + strconv.Itoa(int(bts.Size())) + ",valueSize: " + strconv.Itoa(int(header.ValueSize)))
	}

	value := bts[index:]

	return &Record{
		key,
		value,
		header.Typ,
	}
}
