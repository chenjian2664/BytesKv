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

package memo

import (
	"BytesDB"
	"BytesDB/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIndexManager(t *testing.T) {
	im := NewIndexManager()
	assert.NotNil(t, im)
}

func TestIndexManager_Put(t *testing.T) {
	im := NewIndexManager()
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
	im := NewIndexManager()
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
	im := NewIndexManager()
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
