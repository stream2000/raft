/*
@Time : 2020/4/15 18:04
@Author : Minus4
*/
package raft

import (
	"math/rand"
	"time"
)

// timeoutManager manages timeout event.
type timeoutManager struct {
	timeout int
	rf      *Raft
	rChan   chan struct{}
	cancel  chan struct{}
}

func NewTimeoutManager(rf *Raft, baseTimeout int) (e *timeoutManager) {
	rand.Seed(time.Now().Unix())
	e = &timeoutManager{rf: rf, timeout: baseTimeout}
	e.rChan = make(chan struct{})
	e.cancel = make(chan struct{}, 1)
	return
}

func (m *timeoutManager) start() {
	t := time.NewTimer(m.random())
	defer t.Stop()
	// weather the timeout event have been process
	expired := false
	for {
		select {
		case <-m.rChan:
			if !t.Stop() && !expired {
				// drain the channel
				<-t.C
			}
			m.rf.mu.Lock()
			expired = false
			m.rf.mu.Unlock()
			t.Reset(m.random())
		case <-t.C:
			// timeout event fired
			expired = true
			m.rf.mu.Lock()
			if m.rf.state != Leader {
				m.rf.convertToCandidate()
			}
			m.rf.mu.Unlock()
			t.Reset(m.random())
		case <-m.rf.cancel:
			return
		case <-m.cancel:
			return
		}
	}
}

// we try to restart the timer, if failed, that means the no one is listening or the goroutine is executing other branch
// and may be blocking. Based on the logic of timeoutManager, we can simply discard this message
func (m *timeoutManager) restartTimer() {
	select {
	case m.rChan <- struct{}{}:
	default:
	}
}

// the stop action must be done at least once, so we use a buffered channel
func (m *timeoutManager) stop() {
	select {
	case m.cancel <- struct{}{}:
	default:
	}
}

func (m *timeoutManager) random() time.Duration {
	max := m.timeout + 100
	min := m.timeout - 100
	r := rand.Intn(max-min) + min
	return time.Millisecond * time.Duration(r)
}
