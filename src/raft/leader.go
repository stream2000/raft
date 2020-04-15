/*
@Time : 2020/4/13 11:47
@Author : Minus4
*/
package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// leadershipManager is only created when the raft gains power.
// Then it will manger events as long as the raft server maintains its power
//
// the leadership manager has its own mutex, since the modification of nextIndex has nothing to do with the raft struct
// we will have two other  background goroutines called 'committer' and 'applier', the first one will check matchIndex
// and decide weather to commit a log, the other one will periodically check commitIndex and lastApply field in raft and
// do some persistence job

// committer will read matchIndex to commit log, the response of AppendEntry rpc will modify matchIndex and nextIndex
// all above operation will contend the lock
type leadershipManager struct {
	finished   int32
	term       int // term of this leadershipManager, use to reduce lock contention
	nextIndex  []int
	matchIndex []int
	command    chan LogEntry
	rf         *Raft
	sync.Mutex
}

// a non-block operation
func (l *leadershipManager) stop() {
	atomic.StoreInt32(&l.finished, 1)
}

func NewLeadershipManager(rf *Raft) *leadershipManager {
	l := new(leadershipManager)
	l.term = rf.term
	l.nextIndex = make([]int, len(rf.peers))
	l.matchIndex = make([]int, len(rf.peers))
	l.rf = rf
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		l.nextIndex[i] = len(rf.logs) + 1
		l.matchIndex[i] = len(rf.logs)
	}
	return l
}

func (l *leadershipManager) start() {
	// initial heartbeat

	args := &AppendEntriesReq{
		Term:     l.term,
		LeaderId: l.rf.me,
	}
	for i := range l.rf.peers {
		if i == l.rf.me {
			continue
		}
		go l.appendEntriesHandler(i, args)
	}
	time.Sleep(time.Millisecond * 105)

	for {
		if atomic.LoadInt32(&l.finished) == 1 || l.rf.killed() {
			return
		}
		l.rf.mu.Lock()
		// TODO modify args in Lab2B
		args := &AppendEntriesReq{
			Term:     l.term,
			LeaderId: l.rf.me,
		}
		l.rf.mu.Unlock()
		//	DPrintf("leader %d in term %d is sending heartbeat \n", args.LeaderId, l.term)
		for i := range l.rf.peers {
			if i == l.rf.me {
				continue
			}
			go l.appendEntriesHandler(i, args)
		}
		time.Sleep(time.Millisecond * 105)
	}

}

func (l *leadershipManager) appendEntriesHandler(server int, args *AppendEntriesReq) {
	reply := new(AppendEntriesResp)
	if ok := l.rf.sendAppendEntries(server, args, reply); ok {
		if reply.Term != args.Term {
			l.stop()
			l.rf.mu.Lock()
			term := l.rf.term
			if term < reply.Term {
				l.rf.convertToFollower(reply.Term)
			}
			l.rf.mu.Unlock()
		} else {
		}
	} else {
	}
}
