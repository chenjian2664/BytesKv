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
	"BytesDB/core"
	"BytesDB/index/hash"
	"sync"
)

type IndexType = byte

const (
	Local_Hash IndexType = iota
)

type IndexManager struct {
	indexes map[core.IndexId]core.Index
	mutex   sync.RWMutex
}

func (im *IndexManager) Get(id core.IndexId, key core.Bytes) (*core.RecordPosition, error) {
	return im.resolve(id).Get(key)
}

func (im *IndexManager) Put(id core.IndexId, key core.Bytes, value *core.RecordPosition) (*core.RecordPosition, error) {
	return im.resolve(id).Put(key, value)
}

func (im *IndexManager) Delete(id core.IndexId, key core.Bytes) (bool, error) {
	return im.resolve(id).Delete(key)
}

func (im *IndexManager) ListKeys(id core.IndexId) []core.Bytes {
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

func (im *IndexManager) Iterator(id core.IndexId, reverse bool) (core.Iterator, error) {
	return im.resolve(id).Iterator(reverse)
}

func (im *IndexManager) resolve(id core.IndexId) core.Index {
	if _, ok := im.indexes[id]; !ok {
		im.initializeIndex(Local_Hash, id)
	}
	return im.indexes[id]
}

func (im *IndexManager) initializeIndex(typ IndexType, id core.IndexId) {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	if _, ok := im.indexes[id]; ok {
		return
	}

	switch typ {
	case Local_Hash:
		im.indexes[id] = hash.NewLocalHashIndex()
		return

	default:
		panic("unknown index type")
	}
}
