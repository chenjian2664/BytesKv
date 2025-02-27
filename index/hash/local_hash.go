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
	"BytesDB"
	"BytesDB/core"
)

type LocalHashIndex struct {
	// TODO: add different index types
	index map[string]*core.RecordPosition
}

func NewLocalHashIndex() *LocalHashIndex {
	return &LocalHashIndex{
		index: make(map[string]*core.RecordPosition),
	}
}

func (im *LocalHashIndex) Put(key core.Bytes, record *core.RecordPosition) (*core.RecordPosition, error) {
	if key == nil {
		return nil, BytesDB.ErrKeyIsNil
	}

	if record == nil {
		return nil, BytesDB.ErrRecordPositionNil
	}

	indexKey := string(key)
	if old, ok := im.index[indexKey]; ok {
		return old, nil
	}
	im.index[indexKey] = record
	return nil, nil
}

func (im *LocalHashIndex) Get(key core.Bytes) (*core.RecordPosition, error) {
	if key == nil {
		return nil, BytesDB.ErrKeyIsNil
	}

	if value, ok := im.index[string(key)]; ok {
		return value, nil
	}

	return nil, BytesDB.ErrKeyNotFound
}

func (im *LocalHashIndex) Delete(key core.Bytes) (bool, error) {
	_, err := im.Get(key)
	if err != nil {
		return false, err
	}

	delete(im.index, string(key))
	return true, nil
}

func (im *LocalHashIndex) Exists(key core.Bytes) bool {
	v, err := im.Get(key)
	if err != nil {
		return false
	}

	return v != nil
}
