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

import "encoding/binary"

type HitRecord struct {
	Key Bytes
	Pos RecordPosition
}

func (hr *HitRecord) ToBytes() Bytes {
	buf := make(Bytes, binary.MaxVarintLen64+len(hr.Key)+binary.MaxVarintLen64*2)
	index := 0
	n := binary.PutVarint(buf, int64(len(hr.Key)))
	index += n
	copy(buf[index:], hr.Key)
	index += len(hr.Key)
	n = binary.PutVarint(buf[index:], hr.Pos.Position)
	index += n
	n = binary.PutVarint(buf[index:], int64(hr.Pos.Size))
	index += n
	return buf[:index]
}
