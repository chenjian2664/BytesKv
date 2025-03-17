package utils

import (
	"fmt"
	"strconv"
	"strings"
)

const DataFileSuffix = ".data"

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
