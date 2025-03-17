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

package storage

import (
	"BytesDB/config"
	"BytesDB/core"
	"github.com/stretchr/testify/assert"
	"io"
	"strconv"
	"testing"
)

var sid = core.Session{
	Schema: "public",
	Table:  "test"}

var dbconfig = &config.DBConfig{
	DataDir: "/tmp/bytesdb",
}

func TestNewStorageManager(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	assert.NotNil(t, sm)
}

func TestStorageManager_Write(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	t.Cleanup(func() {
		sm.RemoveAllData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, position, pos.Position)
	assert.Equal(t, len(record.Pack()), pos.Size)

	position += int64(pos.Size)
	record = &core.Record{
		Key:   core.Bytes("你好"),
		Value: core.Bytes("吃了吗"),
		Type:  core.Normal,
	}
	pos = sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, pos.Position, position)
	assert.Equal(t, pos.Size, len(record.Pack()))
}

func TestStorageManager_Read(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	t.Cleanup(func() {
		sm.RemoveAllData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, position, pos.Position)
	assert.Equal(t, len(record.Pack()), pos.Size)

	read := sm.Read(sid, pos)
	assert.Equal(t, record, read)

	position += int64(pos.Size)
	record = &core.Record{
		Key:   core.Bytes("你好"),
		Value: core.Bytes("吃了吗"),
		Type:  core.Normal,
	}
	pos = sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, pos.Position, position)
	assert.Equal(t, pos.Size, len(record.Pack()))
	read = sm.Read(sid, pos)
	assert.Equal(t, record, read)
}

func TestStorageManager_Remove(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	t.Cleanup(func() {
		sm.RemoveAllData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, position, pos.Position)
	assert.Equal(t, len(record.Pack()), pos.Size)

	read := sm.Read(sid, pos)
	assert.Equal(t, record, read)

	// Delete actual write a Deleted type record
	pos = sm.Delete(sid, read.Key)
	deleted := &core.Record{
		Key:   read.Key,
		Value: core.Bytes{},
		Type:  core.Deleted,
	}
	writeDeleted := sm.Read(sid, pos)
	assert.Equal(t, deleted, writeDeleted)
}

func TestStorageManager_Size(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	t.Cleanup(func() {
		sm.RemoveAllData(sid)
	})
	sz, err := sm.Size(sid)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), sz)

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	pos := sm.Write(sid, record)
	sz, err = sm.Size(sid)
	assert.Nil(t, err)
	assert.NotNil(t, pos)
	assert.Equal(t, record.Pack().Size(), uint32(pos.Size))
	assert.Equal(t, int64(record.Pack().Size()), sz)

	_ = sm.Delete(sid, core.Bytes("hello"))
	nsz, err := sm.Size(sid)
	assert.Nil(t, err)
	assert.True(t, nsz > sz)
}

func TestFilePositionIterator(t *testing.T) {
	sm := NewStorageManager(dbconfig)
	t.Cleanup(func() {
		sm.RemoveAllData(sid)
		sm.Close()
	})
	writeSize := 0
	for i := 0; i < 10; i++ {
		rd := &core.Record{
			Key:   core.Bytes(strconv.Itoa(i)),
			Value: core.Bytes("val" + strconv.Itoa(i)),
			Type:  core.Normal,
		}
		sm.Write(sid, rd)
		writeSize += int(rd.Pack().Size())
	}
	sm.Close()

	sm = NewStorageManager(dbconfig)
	storage := sm.resolveStorage(sid)
	iterator, err := storage.PositionIterator()
	assert.Nil(t, err)

	// All normal type writes as expected
	index := 0
	for pos, key, typ, err := iterator.Next(); key != nil; pos, key, typ, err = iterator.Next() {
		if err == io.EOF {
			break
		}
		atoi, _ := strconv.Atoi(string(key))
		assert.Nil(t, err)
		assert.Equal(t, atoi, index)

		assert.Equal(t, sm.Read(sid, pos), &core.Record{
			Key:   core.Bytes(strconv.Itoa(index)),
			Value: core.Bytes("val" + strconv.Itoa(index)),
			Type:  core.Normal,
		})
		assert.Equal(t, typ, core.Normal)

		index++
	}

	// write a deleted record
	sm.Write(sid, &core.Record{
		Key:   core.Bytes(strconv.Itoa(index)),
		Value: core.Bytes("val" + strconv.Itoa(index)),
		Type:  core.Deleted,
	})
	sm.Close()

	sm = NewStorageManager(dbconfig)
	storage = sm.resolveStorage(sid)
	iterator, err = storage.PositionIterator()
	assert.Nil(t, err)

	index = 0
	for pos, key, typ, err := iterator.Next(); key != nil; pos, key, typ, err = iterator.Next() {
		if err == io.EOF {
			break
		}
		if index < 10 {
			atoi, _ := strconv.Atoi(string(key))
			assert.Nil(t, err)
			assert.Equal(t, atoi, index)

			assert.Equal(t, sm.Read(sid, pos), &core.Record{
				Key:   core.Bytes(strconv.Itoa(index)),
				Value: core.Bytes("val" + strconv.Itoa(index)),
				Type:  core.Normal,
			})
			assert.Equal(t, typ, core.Normal)
		} else {
			atoi, _ := strconv.Atoi(string(key))
			assert.Nil(t, err)
			assert.Equal(t, atoi, index)

			assert.Equal(t, sm.Read(sid, pos), &core.Record{
				Key:   core.Bytes(strconv.Itoa(index)),
				Value: core.Bytes("val" + strconv.Itoa(index)),
				Type:  core.Deleted,
			})
			assert.Equal(t, typ, core.Deleted)
		}

		index++
	}
}
