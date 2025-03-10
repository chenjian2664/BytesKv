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

package hash

import (
	"BytesDB/core"
	"errors"
)

type LocalHashIndex struct {
	index    map[string]*core.RecordPosition
	rootPath string
}

type iterator struct {
	keys   []string
	values []*core.RecordPosition
	idx    int
}

func (it *iterator) Rewind() {
	it.idx = 0
}

func (it *iterator) Seek(key core.Bytes) error {
	return errors.New("hash index not support seeking")
}

func (it *iterator) Next() {
	it.idx++
}

func (it *iterator) Valid() bool {
	return it.idx < len(it.keys)
}

func (it *iterator) Key() core.Bytes {
	return core.Bytes(it.keys[it.idx])
}

func (it *iterator) Value() *core.RecordPosition {
	return it.values[it.idx]
}

func (it *iterator) Close() {
	it.keys = nil
	it.values = nil
}

func (idx *LocalHashIndex) Iterator(reverse bool) (core.Iterator, error) {
	if reverse {
		// TODO
		return nil, errors.New("hash index reverse not supported")
	}
	var keys []string
	var values []*core.RecordPosition
	for item := range idx.index {
		keys = append(keys, item)
		values = append(values, idx.index[item])
	}
	return &iterator{
		idx:    0,
		keys:   keys,
		values: values,
	}, nil
}

func NewLocalHashIndex(rootPath string) *LocalHashIndex {
	return &LocalHashIndex{
		index:    make(map[string]*core.RecordPosition),
		rootPath: rootPath,
	}
}

func (idx *LocalHashIndex) Put(key core.Bytes, position *core.RecordPosition) (*core.RecordPosition, error) {
	if key == nil {
		return nil, core.ErrKeyIsNil
	}

	if position == nil {
		return nil, core.ErrRecordPositionNil
	}

	indexKey := string(key)
	if old, ok := idx.index[indexKey]; ok {
		idx.index[indexKey] = position
		return old, nil
	}
	idx.index[indexKey] = position
	return nil, nil
}

func (idx *LocalHashIndex) Get(key core.Bytes) (*core.RecordPosition, error) {
	if key == nil {
		return nil, core.ErrKeyIsNil
	}

	if value, ok := idx.index[string(key)]; ok {
		return value, nil
	}

	return nil, core.ErrKeyNotFound
}

func (idx *LocalHashIndex) Delete(key core.Bytes) (bool, error) {
	_, err := idx.Get(key)
	if err != nil {
		return false, err
	}

	delete(idx.index, string(key))
	return true, nil
}

func (idx *LocalHashIndex) Exists(key core.Bytes) bool {
	v, err := idx.Get(key)
	if err != nil {
		return false
	}

	return v != nil
}
