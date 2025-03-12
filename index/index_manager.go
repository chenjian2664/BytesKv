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

package index

import (
	"BytesDB/config"
	"BytesDB/core"
	"BytesDB/index/hash"
	"os"
	"path/filepath"
	"sync"
)

type IndexType = byte

const (
	Local_Hash IndexType = iota
)

type IndexManager struct {
	indexes map[core.Session]core.Index
	mutex   sync.RWMutex
	typ     IndexType
	dataDir string
}

func NewIndexManager(cfg *config.DBConfig) *IndexManager {
	return &IndexManager{
		indexes: make(map[core.Session]core.Index),
		mutex:   sync.RWMutex{},
		typ:     resolveIndexType(cfg.IndexType),
		dataDir: cfg.DataDir,
	}
}

func (im *IndexManager) Get(id core.Session, key core.Bytes) (*core.RecordPosition, error) {
	return im.resolve(id).Get(key)
}

func (im *IndexManager) Put(id core.Session, key core.Bytes, value *core.RecordPosition) (*core.RecordPosition, error) {
	return im.resolve(id).Put(key, value)
}

func (im *IndexManager) Delete(id core.Session, key core.Bytes) (bool, error) {
	return im.resolve(id).Delete(key)
}

func (im *IndexManager) ListKeys(id core.Session) []core.Bytes {
	im.mutex.RLock()
	defer im.mutex.RUnlock()
	var keys []core.Bytes
	it, _ := im.Iterator(id, false)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	return keys
}

func (im *IndexManager) Iterator(id core.Session, reverse bool) (core.Iterator, error) {
	return im.resolve(id).Iterator(reverse)
}

func (im *IndexManager) RemoveAllData(session core.Session) {
	im.mutex.Lock()
	defer im.mutex.Unlock()
	im.indexes = make(map[core.Session]core.Index)
}

func (im *IndexManager) Close() {
	im.mutex.Lock()
	defer im.mutex.Unlock()
	im.indexes = nil
}

func resolveIndexType(typ string) IndexType {
	// by default
	if typ == "" {
		return Local_Hash
	}
	switch typ {
	case "local_hash":
		return Local_Hash
	default:
		panic("unknown index type")
	}
}

func (im *IndexManager) resolve(id core.Session) core.Index {
	if _, ok := im.indexes[id]; !ok {
		im.initializeIndex(im.typ, id)
	}
	return im.indexes[id]
}

func (im *IndexManager) initializeIndex(typ IndexType, id core.Session) {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	if _, ok := im.indexes[id]; ok {
		return
	}

	switch typ {
	case Local_Hash:
		path := filepath.Join(im.dataDir, id.Schema, id.Table)
		// TODO: remove It's strange to create the dir in index part
		err := os.MkdirAll(path, 0777)
		if err != nil {
			panic(err)
		}
		im.indexes[id] = hash.NewLocalHashIndex(im.dataDir, id.Schema, id.Table)
		return

	default:
		panic("unknown index type")
	}
}
