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

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytes_Compare(t *testing.T) {
	bts1 := make(Bytes, 1)
	bts2 := make(Bytes, 1)
	bts1[0] = 1
	bts2[0] = 1
	assert.Equal(t, bts1, bts2)

	bts1[0] = 2
	assert.Equal(t, 1, bts1.Compare(bts2))

	bts1 = make(Bytes, 2)
	bts1[0] = 1
	bts1[1] = 2

	bts2 = make(Bytes, 2)
	bts2[0] = 1
	bts2[1] = 2
	assert.Equal(t, bts1, bts2)
	assert.Equal(t, 0, bts1.Compare(bts2))
	bts1[1] = 3
	assert.Equal(t, 1, bts1.Compare(bts2))
}

func TestBytes_Size(t *testing.T) {
	bts := make(Bytes, 1)
	assert.Equal(t, uint32(1), bts.Size())

	bts = make(Bytes, 100)
	assert.Equal(t, uint32(100), bts.Size())
}
