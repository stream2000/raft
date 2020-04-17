package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(i, j int) int {
	if i > j {
		return j
	} else {
		return i
	}
}
