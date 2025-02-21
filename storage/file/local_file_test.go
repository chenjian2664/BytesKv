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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLocalFileManager(t *testing.T) {
	f, err := NewLocalFileManager("/tmp/local-file-test")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	// Use t.Cleanup for automatic cleanup after test
	t.Cleanup(func() {
		f.Close()
		os.Remove("/tmp/local-file-test")
	})
}

func TestFileIO_Write(t *testing.T) {
	f, err := NewLocalFileManager("/tmp/local-file-test")
	assert.Nil(t, err)
	assert.NotNil(t, f)

	// Use defer for automatic cleanup
	defer func() {
		f.Close()
		os.Remove("/tmp/local-file-test")
	}()

	n, err := f.Write([]byte("hello world"))
	assert.Nil(t, err)
	assert.Equal(t, 11, n)

	n, err = f.Write([]byte("\nhello world"))
	assert.Nil(t, err)
	assert.Equal(t, 12, n)

	n, err = f.Write([]byte(nil))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
}
