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
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var session = core.Session{
	Schema: "public",
	Table:  "test",
}

func TestOpenBytesDb(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)
}

func TestDatabase_Put_Get(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)
	t.Cleanup(func() {
		db.RemoveAllData(session)
	})

	err := db.Put(session, core.Bytes("hello"), core.Bytes("world"))
	assert.Nil(t, err)
	r, err := db.Get(session, core.Bytes("hello"))
	assert.Nil(t, err)
	assert.Equal(t, core.Bytes("world"), r)

	err = db.Put(session, core.Bytes("hello"), core.Bytes("updated world"))
	assert.Nil(t, err)
	r, _ = db.Get(session, core.Bytes("hello"))
	assert.Equal(t, core.Bytes("updated world"), r)
}

func TestDatabase_Put_Delete(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)
	t.Cleanup(func() {
		db.RemoveAllData(session)
	})

	err := db.Put(session, core.Bytes("hello"), core.Bytes("world"))
	assert.Nil(t, err)
	r, err := db.Get(session, core.Bytes("hello"))
	assert.Nil(t, err)
	assert.Equal(t, core.Bytes("world"), r)

	err = db.Delete(session, core.Bytes("hello"))
	assert.Nil(t, err)
	r, err = db.Get(session, core.Bytes("hello"))
	assert.Nil(t, r)
	assert.Equal(t, err, core.ErrKeyNotFound)

	// delete again
	err = db.Delete(session, core.Bytes("hello"))
	assert.Nil(t, err)
	r, err = db.Get(session, core.Bytes("hello"))
	assert.Nil(t, r)
	assert.Equal(t, err, core.ErrKeyNotFound)

	_ = db.Put(session, core.Bytes("hello"), core.Bytes("world"))
	_ = db.Put(session, core.Bytes("hello"), core.Bytes("world2"))
	_ = db.Put(session, core.Bytes("hello"), core.Bytes("updated world"))
	r, _ = db.Get(session, core.Bytes("hello"))
	assert.Equal(t, core.Bytes("updated world"), r)
	err = db.Delete(session, core.Bytes("hello"))
	assert.Nil(t, err)
	r, err = db.Get(session, core.Bytes("hello"))
	assert.Nil(t, r)
	assert.Equal(t, err, core.ErrKeyNotFound)
}

func TestDatabase_Put_Update_Delete(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)
	t.Cleanup(func() {
		db.RemoveAllData(session)
	})
	err := db.Put(session, core.Bytes("hello"), core.Bytes("world"))
	assert.Nil(t, err)
	r, err := db.Get(session, core.Bytes("hello"))
	assert.Nil(t, err)
	assert.Equal(t, core.Bytes("world"), r)

	err = db.Put(session, core.Bytes("hello"), core.Bytes("updated world"))
	assert.Nil(t, err)
	r, _ = db.Get(session, core.Bytes("hello"))
	assert.Equal(t, core.Bytes("updated world"), r)
	err = db.Delete(session, core.Bytes("hello"))
	assert.Nil(t, err)
	r, err = db.Get(session, core.Bytes("hello"))
	assert.Nil(t, r)
	assert.Equal(t, err, core.ErrKeyNotFound)
}

func TestDatabase_Close(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)

	db.Close()
	assert.Nil(t, db.im)
	assert.Nil(t, db.sm)
}

// test database closed then startup, with the correct data being loaded
func TestDatabase_Startup(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)

	t.Cleanup(func() {
		db.RemoveAllData(session)
	})

	for i := 0; i < 100; i++ {
		_ = db.Put(session, core.Bytes(strconv.Itoa(i)), core.Bytes(strconv.Itoa(i)))
	}
	// test the writing correctly
	for i := 0; i < 100; i++ {
		val, err := db.Get(session, core.Bytes(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, core.Bytes(strconv.Itoa(i)), val)
	}

	db.Close()
	assert.Nil(t, db.im)
	assert.Nil(t, db.sm)

	// reopen db with the same properties
	db = OpenBytesDb()
	assert.NotNil(t, db)
	// test the data exists
	for i := 0; i < 100; i++ {
		val, err := db.Get(session, core.Bytes(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, core.Bytes(strconv.Itoa(i)), val)
	}

	err := db.Delete(session, core.Bytes(strconv.Itoa(100)))
	assert.Nil(t, err)

	db.Close()
	assert.Nil(t, db.im)
	assert.Nil(t, db.sm)

	db = OpenBytesDb()
	assert.NotNil(t, db)

	// reopen
	for i := 0; i < 99; i++ {
		val, err := db.Get(session, core.Bytes(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, core.Bytes(strconv.Itoa(i)), val)
	}
	val, err := db.Get(session, core.Bytes(strconv.Itoa(100)))
	assert.Nil(t, val)
	assert.Equal(t, err, core.ErrKeyNotFound)
}

func TestDatabase_Startup_Delete_Size(t *testing.T) {
	db := OpenBytesDb()
	assert.NotNil(t, db)

	t.Cleanup(func() {
		db.RemoveAllData(session)
	})

	for i := 0; i < 100; i++ {
		_ = db.Put(session, core.Bytes(strconv.Itoa(i)), core.Bytes(strconv.Itoa(i)))
	}
	// test the writing correctly
	for i := 0; i < 100; i++ {
		val, err := db.Get(session, core.Bytes(strconv.Itoa(i)))
		assert.Nil(t, err)
		assert.Equal(t, core.Bytes(strconv.Itoa(i)), val)
	}
	sz, err := db.sm.Size(session)
	assert.Nil(t, err)

	err = db.Delete(session, core.Bytes(strconv.Itoa(0)))
	assert.Nil(t, err)

	nsz, err := db.sm.Size(session)
	assert.Nil(t, err)
	assert.True(t, nsz > sz)

	rd, err := db.Get(session, core.Bytes(strconv.Itoa(0)))
	assert.Nil(t, rd)
	assert.Equal(t, err, core.ErrKeyNotFound)

	err = db.Delete(session, core.Bytes("Key not exists in db"))
	assert.Nil(t, err)
	sz, err = db.sm.Size(session)
	assert.Nil(t, err)
	// Not write the data as the key not exists and stop after the memo index checking
	assert.Equal(t, nsz, sz)
}
