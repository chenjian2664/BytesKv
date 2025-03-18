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

package btree

import (
	"BytesDB/core"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type Item struct {
	key core.Bytes
	pos *core.RecordPosition
}

func (it *Item) Less(than btree.Item) bool {
	return bytes.Compare(it.key, than.(*Item).key) < 0
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key core.Bytes, pos *core.RecordPosition) (*core.RecordPosition, error) {
	if key == nil {
		return nil, core.ErrKeyIsNil
	}
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	res := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if res == nil {
		return nil, nil
	}
	return res.(*Item).pos, nil
}

func (bt *BTree) Get(key core.Bytes) (*core.RecordPosition, error) {
	if key == nil {
		return nil, core.ErrKeyIsNil
	}

	it := &Item{key: key}
	item := bt.tree.Get(it)
	if item == nil {
		return nil, core.ErrKeyNotFound
	}
	return item.(*Item).pos, nil
}

func (bt *BTree) Delete(key core.Bytes) (bool, error) {
	if key == nil {
		return false, core.ErrKeyIsNil
	}

	it := &Item{key: key}
	bt.lock.Lock()
	old := bt.tree.Delete(it)
	bt.lock.Unlock()
	if old != nil {
		return true, nil
	}
	return false, nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Exists(key core.Bytes) bool {
	return bt.tree.Get(&Item{key: key}) != nil
}

func (bt *BTree) Iterator(reverse bool) (core.Iterator, error) {
	if bt.tree == nil {
		return nil, nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse), nil
}

func (bt *BTree) Close() error {
	return nil
}

// BTree index iterator
type btreeIterator struct {
	currentIndex int
	reverse      bool // the direction, false: left->right, true right->left
	values       []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		0,
		reverse,
		values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.currentIndex = 0
}

func (bti *btreeIterator) Seek(key core.Bytes) error {
	if bti.reverse {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].key) >= 0
		})
	} else {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].key) <= 0
		})
	}
	return nil
}

func (bti *btreeIterator) Next() {
	bti.currentIndex++
}

func (bti *btreeIterator) Valid() bool {
	return bti.currentIndex < len(bti.values)
}

func (bti *btreeIterator) Key() core.Bytes {
	return bti.values[bti.currentIndex].key
}

func (bti *btreeIterator) Value() *core.RecordPosition {
	return bti.values[bti.currentIndex].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
