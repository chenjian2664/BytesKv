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

import "BytesDB/core"

type IndexType = byte

const (
	Local_Hash IndexType = iota
)

type IndexManager struct {
	indexes map[core.IndexId]*core.Index
}

func (im *IndexManager) Get(id core.IndexId, key core.Bytes) (core.Bytes, error) {
	if _, ok := im.indexes[id]; !ok {
		panic("index not found")
	}

	return nil, nil
}

func (im *IndexManager) Put(id core.IndexId, key core.Bytes, value core.Bytes) error {
	return nil
}

func (im *IndexManager) Delete(id core.IndexId, key core.Bytes) error {
	return nil
}

func (im *IndexManager) ListKeys(id core.IndexId) []core.Bytes {
	return nil
}

func (im *IndexManager) Iter(id core.IndexId) core.Iterator {
	return nil
}
