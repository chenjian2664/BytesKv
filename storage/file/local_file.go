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

package file

import (
	"BytesDB/storage"
	"os"
)

// FileIO FilePerm defines default file permissions (readable by everyone, writable by owner)
type FileIO struct {
	fd *os.File
}

func NewLocalFileManager(filePath string) (storage.StorageManager, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, storage.FilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: file}, nil
}

func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}

func (fio *FileIO) Write(buf []byte) (int, error) {
	return fio.fd.Write(buf)
}

func (fio *FileIO) Flush() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
