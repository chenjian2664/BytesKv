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

package hash

import (
	"BytesDB"
	"BytesDB/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIndexManager(t *testing.T) {
	im := NewLocalHashIndex()
	assert.NotNil(t, im)
}

func TestIndexManager_Put(t *testing.T) {
	im := NewLocalHashIndex()
	assert.NotNil(t, im)

	pos := &core.RecordPosition{
		StorageId: core.StorageId{
			Schema:    "public",
			Table:     "test",
			StorageId: "000000000001.data"},
		Size: 10,
	}

	old, err := im.Put(core.Bytes("hello"), pos)
	assert.Nil(t, err)
	assert.Nil(t, old)

	old, err = im.Put(core.Bytes("hello"), nil)
	assert.Nil(t, old)
	assert.Error(t, err, BytesDB.ErrRecordPositionNil)

	old, err = im.Put(core.Bytes("hello"), &core.RecordPosition{})
	assert.Nil(t, err)
	assert.Equal(t, old, pos)

	v, err := im.Put(nil, &core.RecordPosition{})
	assert.Nil(t, v)
	assert.Error(t, err, BytesDB.ErrKeyIsNil)
}

func TestIndexManager_Get(t *testing.T) {
	im := NewLocalHashIndex()
	assert.NotNil(t, im)

	pos := &core.RecordPosition{
		StorageId: core.StorageId{
			Schema:    "public",
			Table:     "test",
			StorageId: "000000000001.data"},
		Size: 10,
	}

	old, err := im.Put(core.Bytes("hello"), pos)
	assert.Nil(t, err)
	assert.Nil(t, old)

	v, err := im.Get(core.Bytes("hello"))
	assert.Nil(t, err)
	assert.Equal(t, pos, v)

	v, err = im.Get(core.Bytes("not exists"))
	assert.Nil(t, v)
	assert.Error(t, err, BytesDB.ErrKeyNotFound)

	v, err = im.Get(nil)
	assert.Nil(t, v)
	assert.Error(t, err, BytesDB.ErrKeyIsNil)
}

func TestIndexManager_Delete(t *testing.T) {
	im := NewLocalHashIndex()
	assert.NotNil(t, im)

	pos := &core.RecordPosition{}
	old, err := im.Put(core.Bytes("hello"), pos)
	assert.Nil(t, err)
	assert.Nil(t, old)

	v, err := im.Get(core.Bytes("hello"))
	assert.Nil(t, err)
	assert.Equal(t, pos, v)

	deleted, err := im.Delete(core.Bytes("hello"))
	assert.Nil(t, err)
	assert.True(t, deleted)

	v, err = im.Get(core.Bytes("not exists"))
	assert.Nil(t, v)
	assert.Error(t, err, BytesDB.ErrKeyNotFound)

	deleted, err = im.Delete(nil)
	assert.False(t, deleted)
	assert.Error(t, err, BytesDB.ErrKeyIsNil)
}

func TestLocalHashIndex_Iterator(t *testing.T) {
	im := NewLocalHashIndex()
	assert.NotNil(t, im)

	it, err := im.Iterator(false)
	assert.Nil(t, err)
	assert.NotNil(t, it)

	it, err = im.Iterator(true)
	assert.Nil(t, it)
	assert.Error(t, err, "hash index reverse not supported")

	pos := &core.RecordPosition{
		StorageId: core.StorageId{
			Schema:    "public",
			Table:     "test",
			StorageId: "000000000000.data"},
		Position: int64(0),
		Size:     5,
	}

	write, err := im.Put(core.Bytes("hello0"), pos)
	assert.Nil(t, err)
	assert.Nil(t, write)

	// the old value is return correctly
	write, err = im.Put(core.Bytes("hello0"), pos)
	assert.Nil(t, err)
	assert.NotNil(t, write)
	assert.Equal(t, write, pos)

	// Test all keys are correct accessed by iterator
	keys := make(map[string]*core.RecordPosition)
	keys["hello0"] = pos

	_, _ = im.Put(core.Bytes("hello1"), pos)
	keys["hello1"] = pos

	_, _ = im.Put(core.Bytes("hello2"), pos)
	keys["hello2"] = pos

	_, _ = im.Put(core.Bytes("hello3"), pos)
	keys["hello3"] = pos

	it, err = im.Iterator(false)
	assert.Nil(t, err)
	for i := 0; it.Valid(); it.Next() {
		assert.Equal(t, pos, it.Value())
		expected, ok := keys[string(it.Key())]
		assert.True(t, ok)
		assert.Equal(t, expected, it.Value())
		i++
	}
}
