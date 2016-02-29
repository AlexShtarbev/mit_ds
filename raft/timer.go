package raft

import (
	"time"
	"math/rand"
	"strconv"
)

const (
	minWaitInterval  = 0
	maxWaitInterval = 100
)

type Timer struct {
	duration int
	HeartbeatReceived chan bool
	StartElectionChan chan bool
	ticker *time.Ticker
	me int
}

func (t *Timer) NewTimer(me int) {
	t.me = me

	if(t.HeartbeatReceived == nil) {
		t.HeartbeatReceived = make(chan bool)
	}

	if(t.StartElectionChan == nil) {
		t.StartElectionChan = make(chan bool)
	}

	t.ResetTimer()

	go t.waitForElection()
}

func (t *Timer) ResetTimer(){
	if(t.ticker != nil) {
		t.ticker.Stop()
	}
	rand.Seed(time.Now().UnixNano())
	t.duration = rand.Intn(maxWaitInterval - minWaitInterval) + 20
	t.ticker = time.NewTicker(time.Duration(t.duration)*time.Millisecond)
	//printOut(strconv.Itoa(t.me) + " duration:" + strconv.Itoa(t.duration))
}

func (t *Timer) waitForElection() {
	for {
		select {
		case <- t.HeartbeatReceived:
			{
				//printOut("at: " + strconv.Itoa(t.me) + " --> received heartbeat")
				t.ResetTimer()
				//printOut(strconv.Itoa(t.me) + ": elapsed time" + strconv.Itoa(t.elapsedTime) + "/" + strconv.Itoa(t.duration))
			}
		case <- t.ticker.C:
			{
				printOut(strconv.Itoa(t.me) + " --> start election after " + strconv.Itoa(t.duration) + "ms timeout" )
				t.ticker.Stop();
				t.StartElectionChan <- true
			}
		}
		//printOut(strconv.Itoa(t.me) + ": elapsed time" + strconv.Itoa(elapsedTime) + "/" + strconv.Itoa(t.duration))
	}

}
