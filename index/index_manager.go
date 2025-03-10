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
	cfg     *config.DBConfig
}

func NewIndexManager(cfg *config.DBConfig) *IndexManager {
	return &IndexManager{
		indexes: make(map[core.Session]core.Index),
		mutex:   sync.RWMutex{},
		cfg:     cfg,
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

func (im *IndexManager) resolve(id core.Session) core.Index {
	if _, ok := im.indexes[id]; !ok {
		im.initializeIndex(Local_Hash, id)
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
		path := filepath.Join(im.cfg.DataDir, id.Schema, id.Table)
		im.indexes[id] = hash.NewLocalHashIndex(path)
		return

	default:
		panic("unknown index type")
	}
}
