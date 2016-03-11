package raft

import (
	"log"
	"os"
	"fmt"
	"time"
	"math/rand"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func printOut(s string) {
	f, _ := os.OpenFile("out", os.O_RDWR|os.O_APPEND, 0660)
	defer f.Close();

	f.WriteString(s + "\n")
	fmt.Printf(s + "\n")
}


func flushOut() {
	f, _ := os.Create("out")
	f.Close();
}

func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	randomDelay := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + randomDelay)
}
// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// backoff is used to compute an exponential backoff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}

	return base

}

// asyncNotifyCh is used to do an async channel send
// to a single channel without blocking.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}