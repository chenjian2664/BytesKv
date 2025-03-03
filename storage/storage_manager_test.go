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
	"BytesDB/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sid = core.StorageId{
	Schema: "public",
	Table:  "test"}

func TestNewStorageManager(t *testing.T) {
	sm := NewStorageManager()
	assert.NotNil(t, sm)
}

func TestStorageManager_Write(t *testing.T) {
	sm := NewStorageManager()
	t.Cleanup(func() {
		sm.RemoveStorageData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, sid, pos.StorageId)
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
	assert.Equal(t, pos.StorageId, sid)
	assert.Equal(t, pos.Position, position)
	assert.Equal(t, pos.Size, len(record.Pack()))
}

func TestStorageManager_Read(t *testing.T) {
	sm := NewStorageManager()
	t.Cleanup(func() {
		sm.RemoveStorageData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, sid, pos.StorageId)
	assert.Equal(t, position, pos.Position)
	assert.Equal(t, len(record.Pack()), pos.Size)

	read := sm.Read(pos)
	assert.Equal(t, record, read)

	position += int64(pos.Size)
	record = &core.Record{
		Key:   core.Bytes("你好"),
		Value: core.Bytes("吃了吗"),
		Type:  core.Normal,
	}
	pos = sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, pos.StorageId, sid)
	assert.Equal(t, pos.Position, position)
	assert.Equal(t, pos.Size, len(record.Pack()))
	read = sm.Read(pos)
	assert.Equal(t, record, read)
}

func TestStorageManager_Remove(t *testing.T) {
	sm := NewStorageManager()
	t.Cleanup(func() {
		sm.RemoveStorageData(sid)
	})

	record := &core.Record{
		Key:   core.Bytes("hello"),
		Value: core.Bytes("world!"),
		Type:  core.Normal,
	}

	var position int64

	pos := sm.Write(sid, record)
	assert.NotNil(t, pos)
	assert.Equal(t, sid, pos.StorageId)
	assert.Equal(t, position, pos.Position)
	assert.Equal(t, len(record.Pack()), pos.Size)

	read := sm.Read(pos)
	assert.Equal(t, record, read)

	// Delete actual write a Deleted type record
	pos = sm.Delete(sid, read.Key)
	deleted := &core.Record{
		Key:   read.Key,
		Value: core.Bytes{},
		Type:  core.Deleted,
	}
	writeDeleted := sm.Read(pos)
	assert.Equal(t, deleted, writeDeleted)
}
