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

package BytesDB

import (
	"BytesDB/core"
	"BytesDB/index"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
)

const NonTxnSeqNo uint64 = 0

var txnFinishKey = []byte("txn-f")

type WriteBatchOptions struct {
	MaxBatchSize int
	// Mark if flush data to disk when commit the batch writes
	SyncWrites bool
}

type WriteBatch struct {
	options       WriteBatchOptions
	lock          *sync.Mutex
	db            *Database
	pendingWrites map[string]*core.Record
}

// NewWriteBatch Initialize for batch writes
func (db *Database) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if index.ResolveIndexType(db.options.IndexType) == index.BTree {
		panic("can not use write batch, seq no file not exists")
	}

	return &WriteBatch{
		options:       opts,
		lock:          &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*core.Record),
	}
}
func (wb *WriteBatch) Put(key core.Bytes, value core.Bytes) error {
	if len(key) == 0 {
		return core.ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	// Save log record
	record := &core.Record{
		Key:   key,
		Value: value,
		Type:  core.Normal,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Delete(session core.Session, key core.Bytes) error {
	if len(key) == 0 {
		return core.ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	pos, _ := wb.db.im.Get(session, key)
	if pos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// save log record
	record := &core.Record{
		Key:  key,
		Type: core.Deleted,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

// Commit commit transaction, write data
func (wb *WriteBatch) Commit(session core.Session) error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > wb.options.MaxBatchSize {
		return errors.New("can commit, too large")
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	positions := make(map[string]*core.RecordPosition)
	// Get transaction sequence number
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	for _, record := range wb.pendingWrites {
		key := logRecordKeyWithSeq(record.Key, seqNo)
		pos, err := appendLogRecord(&core.Record{
			Key:   key,
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}

	commited := &core.Record{
		Key:  logRecordKeyWithSeq(txnFinishKey, seqNo),
		Type: core.TxnFinished,
	}

	if _, err := appendLogRecord(commited); err != nil {
		return err
	}

	// TODO: Flush according to the config

	// Update memo index
	for _, record := range wb.pendingWrites {

		pos := positions[string(record.Key)]
		if record.Type == core.Normal {
			wb.db.im.Put(session, record.Key, pos)
		}
		if record.Type == core.Deleted {
			wb.db.im.Delete(session, record.Key)
		}
	}

	// TODO: update writing size

	// Clean pending writes
	wb.pendingWrites = make(map[string]*core.Record)
	return nil
}

func logRecordKeyWithSeq(key core.Bytes, seqNo uint64) core.Bytes {
	seq := make(core.Bytes, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make(core.Bytes, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// Get actual key
func parseLogRecordKey(key core.Bytes) (core.Bytes, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}

func appendLogRecord(record *core.Record) (*core.RecordPosition, error) {
	return nil, nil
}
