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
	"BytesDB/config"
	"BytesDB/core"
	"BytesDB/index"
	"BytesDB/storage"
	"errors"
)

type Database struct {
	options *config.DBConfig
	im      *index.IndexManager
	sm      *storage.StorageManager
	seqNo 	uint64
}

func OpenBytesDb() *Database {
	// TODO: implement it
	cfg, err := config.LoadConfig("db.properties")
	if err != nil {
		panic(err)
	}

	return &Database{
		options: cfg,
		im:      index.NewIndexManager(cfg),
		sm:      storage.NewStorageManager(cfg),
	}
}

func (db *Database) Put(Session core.Session, key, value core.Bytes) error {
	record := &core.Record{
		Key:   key,
		Value: value,
		Type:  core.Normal,
	}
	pos := db.sm.Write(Session, record)
	_, err := db.im.Put(Session, key, pos)
	return err
}

func (db *Database) Get(session core.Session, key core.Bytes) (core.Bytes, error) {
	pos, err := db.im.Get(session, key)
	if err != nil {
		return nil, err
	}

	if pos == nil {
		panic("error get position, supposed to return error")
	}

	record := db.sm.Read(session, pos)
	return record.Value, nil
}

func (db *Database) Delete(session core.Session, key core.Bytes) error {
	pos, err := db.im.Get(session, key)
	if err != nil && !errors.Is(err, core.ErrKeyNotFound) {
		return err
	}

	if pos == nil {
		return nil
	}

	db.sm.Delete(session, key)
	_, err = db.im.Delete(session, key)
	return err
}

func (db *Database) Keys(session core.Session) []core.Bytes {
	return db.im.ListKeys(session)
}

// RemoveAllData Note this only for test
func (db *Database) RemoveAllData(session core.Session) {
	if db.im != nil {
		db.im.RemoveAllData(session)
	}

	if db.sm != nil {
		db.sm.RemoveAllData(session)
	}
}

func (db *Database) Close() {
	db.sm.Close()
	db.sm = nil

	db.im.Close()
	db.im = nil
}