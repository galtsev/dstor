package util

import (
	. "dan/pimco/base"
	"strconv"
	"strings"
)

func PartitionsToStr(partitions []int32) string {
	strPartitions := make([]string, len(partitions))
	for i, p := range partitions {
		strPartitions[i] = strconv.Itoa(int(p))
	}
	return strings.Join(strPartitions, ",")
}

func ParsePartitions(s string) []int32 {
	strPartitions := strings.Split(s, ",")
	partitions := make([]int32, len(strPartitions))
	for i, sp := range strPartitions {
		p, err := strconv.Atoi(sp)
		Check(err)
		partitions[i] = int32(p)
	}
	return partitions
}
