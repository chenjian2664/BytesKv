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
