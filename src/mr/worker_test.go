/*
@Time : 2020/4/8 11:30
@Author : Minus4
*/
package mr

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
)

func Test_ProcessMap(t *testing.T) {
	mTask := mapTask{
		File:     "pg-grimm.txt",
		ReduceN:  4,
		MapOrder: 0,
	}
	processMapTask(mTask, Map)
}

func Test_ProcessReduce(t *testing.T) {
	mTask := reduceTask{
		MapN:        1,
		ReduceOrder: 0,
	}
	processReduceTask(mTask, Reduce)
}
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
