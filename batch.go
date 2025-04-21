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
	"sync"
)

const NonTxnSeqNo uint64 = 0

var TxnFinishKey = []byte("txn-f")

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

func (wb *WriteBatch) Delete(key core.Bytes) error {
	return nil
}

func (wb *WriteBatch) Commit() error {
	return nil
}
