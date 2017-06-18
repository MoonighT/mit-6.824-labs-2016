package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug > 0 {
		fmt.Printf(format, a...)
		fmt.Printf("\n")
	}
	return
}
