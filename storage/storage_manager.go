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
	"BytesDB/storage/file"
	"sync"
)

type StorageType = byte

const (
	Local_File StorageType = iota
)

type StorageManager struct {
	storages map[core.StorageId]core.Storage
	mutex    sync.RWMutex
	options  StorageOptions
}

func NewStorageManager() *StorageManager {
	return &StorageManager{
		make(map[core.StorageId]core.Storage),
		sync.RWMutex{},
		loadStorageOptions(),
	}
}

func loadStorageOptions() StorageOptions {
	// TODO: implement it
	return StorageOptions{}
}

func (sm *StorageManager) Read(position core.RecordPosition) *core.Record {
	sid := position.StorageId
	if _, ok := sm.storages[sid]; !ok {
		sm.initializeStorage(Local_File, sid)
	}
	// TODO
	return nil
}

// append
func (sm *StorageManager) Write(record core.Record) core.RecordPosition {
	return core.RecordPosition{}
}

func (sm *StorageManager) Delete(key core.Bytes) {
}

func (sm *StorageManager) initializeStorage(storageType StorageType, storageId core.StorageId) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if _, ok := sm.storages[storageId]; ok {
		return
	}
	switch storageType {
	case Local_File:
		var path string
		if root, ok := sm.options.rootPaths[storageId.Schema]; !ok {
			// TODO: add it into options
			path = "/var/bytes_db/warehouse"
		} else {
			path = root
		}

		storage, err := file.NewLocalFileStorage(path, "public", "test")
		if err != nil {
			panic(err)
		}
		sm.storages[storageId] = storage
	default:
		// TODO: create error
		panic("storage type not supported: " + string(storageType))
	}
}
