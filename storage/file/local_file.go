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
	"BytesDB/core"
	"BytesDB/utils"
	"encoding/binary"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// TODO: shall we check storageId? we are writing active file only

// fileStorage FilePerm defines default file permissions (readable by everyone, writable by owner)
type fileStorage struct {
	activeFile *os.File
	oldFiles   []string
	rootPath   string
	schema     string
	tableName  string
	maxSize    int64
	mutex      sync.RWMutex
}

func NewLocalFileStorage(rootPath, schema, table string) (core.Storage, error) {
	dir := path.Join(rootPath, schema, table)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			// TODO: better message
			panic(err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	var fileNames []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), utils.DataFileSuffix) {
			fileNames = append(fileNames, entry.Name())
		} else if strings.HasSuffix(entry.Name(), utils.HitFileSuffix) {
			// skip
		} else {
			panic("Unexpected file: " + entry.Name())
		}
	}
	sort.Strings(fileNames)

	var activePath string
	if len(fileNames) == 0 {
		activePath = dir + "/" + utils.BuildDataFileName(0)
	} else {
		activePath = dir + "/" + fileNames[len(fileNames)-1]
	}
	// note: append mode
	activeFile, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		panic(err)
	}

	return &fileStorage{
		activeFile: activeFile,
		oldFiles:   fileNames,
		rootPath:   rootPath,
		schema:     schema,
		tableName:  table,
		// 1MB
		maxSize: 1024 * 1024,
		mutex:   sync.RWMutex{}}, nil
}

func (fio *fileStorage) createAndResetActiveFile() {
	fio.mutex.Lock()
	defer fio.mutex.Unlock()

	old := fio.activeFile.Name()
	_ = fio.Flush()

	err := fio.Close()
	if err != nil {
		panic(err)
	}

	fio.oldFiles = append(fio.oldFiles, old)

	oldSeq := utils.GetFileSeqNo(old)
	nextSeq := oldSeq + 1
	activePath := path.Join(fio.rootPath, fio.schema, fio.tableName, utils.BuildDataFileName(nextSeq))
	// note: append mode
	activeFile, err := os.OpenFile(activePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		panic(err)
	}
	fio.activeFile = activeFile

	// write hit file
	oldIt := &PositionIterator{
		files:   []string{old},
		dataDir: path.Join(fio.rootPath, fio.schema, fio.tableName),
	}

	hitPath := path.Join(fio.rootPath, fio.schema, fio.tableName, utils.BuildHitFileName(oldSeq))
	hitFile, err := os.OpenFile(hitPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		panic(err)
	}
	defer hitFile.Close()

	// TODO: save it in index
	hits := make(map[string]core.RecordPosition)
	for {
		pos, key, typ, err := oldIt.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if typ == core.Deleted {
			delete(hits, string(key))
		} else {
			hits[string(key)] = *pos
		}
	}
	keys := make([]string, 0)
	for k := range hits {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := hits[k]
		buf := make(core.Bytes, binary.MaxVarintLen64+len(k)+binary.MaxVarintLen64*2)
		index := 0
		n := binary.PutVarint(buf, int64(len(k)))
		index += n
		copy(buf[index:], k)
		index += len(k)
		n = binary.PutVarint(buf[index:], v.Position)
		index += n
		n = binary.PutVarint(buf[index:], int64(v.Size))
		index += n

		_, err := hitFile.Write(buf[:index])
		if err != nil {
			panic(err)
		}
	}
}

func (fio *fileStorage) Read(buf core.Bytes, offset int64) (int, error) {
	return fio.activeFile.ReadAt(buf, offset)
}

func (fio *fileStorage) Write(buf core.Bytes) (int, error) {
	size, err := fio.Size()
	if err != nil {
		panic(err)
	}
	if size+int64(len(buf)) > fio.maxSize {
		err := fio.Flush()
		if err != nil {
			// TODO
			panic(err)
		}
		err = fio.Flush()
		if err != nil {
			panic(err)
		}
		fio.createAndResetActiveFile()
	}

	return fio.activeFile.Write(buf)
}

func (fio *fileStorage) Flush() error {
	return fio.activeFile.Sync()
}

func (fio *fileStorage) Close() error {
	return fio.activeFile.Close()
}

func (fio *fileStorage) PositionIterator() (core.PositionIterator, error) {
	return &PositionIterator{
		files:   fio.oldFiles,
		dataDir: path.Join(fio.rootPath, fio.schema, fio.tableName),
	}, nil
}

func (fio *fileStorage) Size() (int64, error) {
	stat, err := fio.activeFile.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (fio *fileStorage) RemoveAll() error {
	path := path.Join(fio.rootPath, fio.schema, fio.tableName)
	return os.RemoveAll(path)
}

func (fio *fileStorage) CleanAll(id core.Session) error {
	tableLocation := path.Join(fio.rootPath, id.Schema, id.Table)
	return os.RemoveAll(tableLocation)
}

type PositionIterator struct {
	index   int
	pos     int
	files   []string
	dataDir string
	cur     *os.File
}

// TODO: support read single file
func (fpi *PositionIterator) Next() (*core.RecordPosition, core.Bytes, core.RecordType, error) {
	if fpi.index >= len(fpi.files) {
		return nil, nil, core.Deleted, io.EOF
	}

	if fpi.cur == nil {
		file, err := os.Open(filepath.Join(fpi.dataDir, fpi.files[fpi.index]))
		if err != nil {
			return nil, nil, core.Deleted, err
		}
		fpi.cur = file
		fpi.pos = 0
	}

	pos := fpi.pos
	stat, err := fpi.cur.Stat()
	if err != nil {
		panic(err)
	}
	if int64(pos+5) >= stat.Size() {
		fpi.index += 1
		fpi.cur = nil
		return fpi.Next()
	}

	// Read the data
	buf := make(core.Bytes, core.MaxLogRecordHeaderSize)
	n, err := fpi.cur.ReadAt(buf, int64(pos))
	if err != nil && err != io.EOF {
		return nil, nil, core.Deleted, err
	}

	if n > 0 {
		// decode buf[:n]
		// [0,4) crc
		// 4 type
		// [4, x) keySize
		// [x, y) valueSize
		typ := core.RecordType(buf[4])

		index := 5
		keySize, n := binary.Varint(buf[index:])
		index += n
		valueSize, n := binary.Varint(buf[index:])
		index += n

		buf = make(core.Bytes, keySize)
		_, err := fpi.cur.ReadAt(buf, int64(pos+index))
		if err != nil {
			return nil, nil, core.Deleted, err
		}

		index += int(keySize) + int(valueSize)

		fpi.pos += index

		return &core.RecordPosition{
			Position: int64(pos),
			Size:     index,
		}, buf, typ, nil
	}

	return nil, nil, core.Deleted, io.EOF
}
