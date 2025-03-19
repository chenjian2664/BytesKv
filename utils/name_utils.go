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

package utils

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	DataFileSuffix = ".data"
	HitFileSuffix  = ".hit"
)

func BuildDataFileName(seqNo int64) string {
	return fmt.Sprintf("%10d"+DataFileSuffix, seqNo)
}

func GetFileSeqNo(path string) int64 {
	if !strings.HasPrefix(path, DataFileSuffix) {
		panic("the file is not a bytesdb data file")
	}
	seqNo, err := strconv.ParseInt(path[:len(path)-5], 10, 64)
	if err != nil {
		panic(err)
	}
	return seqNo
}
