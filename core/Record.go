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

type RecordType byte

const (
	// LogRecordNormal normal append data
	LogRecordNormal RecordType = iota
	// LogRecordDeleted the record is deleted
	LogRecordDeleted
)

// Record the record that use between memo and storage
type Record struct {
	Key   Bytes
	Value Bytes
	Type  RecordType
}

// crc type keySize valueSize
// 4    1    5 			5
// const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2 = 15

// RecordHeader the header of the record
type RecordHeader struct {
	Crc       uint32
	Typ       RecordType
	KeySize   uint32
	ValueSize uint32
}

// RecordPosition the position of the record
// use it to read actual data from storage
type RecordPosition struct {
	StorageId Bytes
	Position  uint64
	Size      uint32
}
