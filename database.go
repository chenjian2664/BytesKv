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
	"BytesDB/storage"
	"errors"
)

type Database struct {
	im *index.IndexManager
	sm *storage.StorageManager
}

func OpenBytesDb() *Database {
	// TODO: implement it

	return &Database{
		index.NewIndexManager(),
		storage.NewStorageManager(),
	}
}

func (db *Database) Put(schema, table string, key, value core.Bytes) error {
	sid := core.StorageId{
		Schema: schema,
		Table:  table,
	}

	iid := core.IndexId{
		Schema: schema,
		Table:  table,
	}

	record := &core.Record{
		Key:   key,
		Value: value,
		Type:  core.Normal,
	}
	pos := db.sm.Write(sid, record)
	_, err := db.im.Put(iid, key, &pos)
	return err
}

func (db *Database) Get(schema, table string, key core.Bytes) (core.Bytes, error) {
	pos, err := db.im.Get(core.IndexId{
		Schema: schema,
		Table:  table,
	}, key)
	if err != nil {
		return nil, err
	}

	if pos == nil {
		panic("error get position, supposed to return error")
	}

	record := db.sm.Read(*pos)
	return record.Value, nil
}

func (db *Database) Delete(schema, table string, key core.Bytes) error {
	iid := core.IndexId{Schema: schema, Table: table}
	pos, err := db.im.Get(iid, key)
	if err != nil && !errors.Is(err, core.ErrKeyNotFound) {
		return err
	}

	if pos == nil {
		return nil
	}

	sid := core.StorageId{Schema: schema, Table: table}
	db.sm.Delete(sid, key)
	_, err = db.im.Delete(iid, key)
	return err
}

func (db *Database) Keys(schema, table string) []core.Bytes {
	return db.im.ListKeys(core.IndexId{Schema: schema, Table: table})
}

func (db *Database) Clear() {
	// TODO: implement it
}

func (db *Database) Close() {
	// TODO: implement it
}
