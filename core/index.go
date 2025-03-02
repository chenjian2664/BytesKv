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

package core

type Index interface {
	Put(Bytes, *RecordPosition) (*RecordPosition, error)
	Get(Bytes) (*RecordPosition, error)
	Delete(Bytes) (bool, error)
	Exists(Bytes) bool

	Iterator(reverse bool) (Iterator, error)
}

type Iterator interface {
	// Rewind Back to the first data
	Rewind()
	// Seek find first key greater or equals to the `key`
	Seek(key Bytes) error
	Next()
	Valid() bool
	Key() Bytes
	Value() *RecordPosition
	Close()
}

type IndexId struct {
	Schema string
	Table  string
}
