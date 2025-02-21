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

const FilePerm = 0644

type FileIoType = byte

const (
	StandardFIO FileIoType = iota
)

type StorageManager interface {
	// Read from file with the position
	Read([]byte, int64) (int, error)

	// Write to the file with the position
	Write([]byte) (int, error)

	// Flush refresh memo data into storage
	Flush() error

	// Close the storage manager
	Close() error

	// Size Get File size
	Size() (int64, error)
}
