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
