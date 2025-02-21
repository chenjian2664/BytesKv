package file

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewLocalFileManager(t *testing.T) {
	f, err := NewLocalFileManager("/tmp/local-file-test")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	err = os.Remove("/tmp/local-file-test")
	if err != nil {
		panic(err)
	}
}
