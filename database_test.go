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
