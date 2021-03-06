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
	finished    int32
	term        int // term of this leadershipManager, use to reduce lock contention
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	rf          *Raft
	mu          sync.Mutex
	c           *committer
}

// stop the leadership manager by set the finished flag
func (l *leadershipManager) stop() {
	atomic.StoreInt32(&l.finished, 1)
}

// start will start the leadership manager daemon
func (l *leadershipManager) start(initArg *AppendEntriesReq) {
	for i := range l.rf.peers {
		if i == l.rf.me {
			continue
		}
		go l.appendEntriesHandler(i, initArg)
	}
	time.Sleep(time.Millisecond * HeartbeatInterval)
	for {
		if atomic.LoadInt32(&l.finished) == 1 || l.rf.killed() {
			return
		}
		//	DPrintf("leader %d in term %d is sending heartbeat \n", args.LeaderId, l.term)
		l.mu.Lock()
		l.rf.mu.Lock()
		if l.term != l.rf.Term {
			l.rf.mu.Unlock()
			return
		}
		for i := range l.rf.peers {
			if i == l.rf.me {
				continue
			}
			next := l.nextIndex[i]
			args := &AppendEntriesReq{
				Term:            l.term,
				LeaderId:        l.rf.me,
				LeaderCommitted: l.commitIndex,
				Entries:         l.rf.Logs[next-1:],
			}
			//DPrintf("append: server %d send to %d  from index %d to %d \n", l.rf.me, i, next, next+len(args.Entries)-1)
			//DPrintf("append: server %d send to %d\n", l.rf.me, i)
			//DPrintf("args : %+v", *args)
			if next != 1 {
				args.PrevLogIndex = next - 1
				args.PrevLogTerm = l.rf.Logs[next-2].Term
			}
			go l.appendEntriesHandler(i, args)
		}
		l.rf.mu.Unlock()
		l.mu.Unlock()
		time.Sleep(time.Millisecond * HeartbeatInterval)
	}
}

func (l *leadershipManager) appendEntriesHandler(server int, args *AppendEntriesReq) {
	reply := new(AppendEntriesResp)
	//defer DPrintf("args %+v to %d reply : %+v\n", args, server, reply)
	if ok := l.rf.sendAppendEntries(server, args, reply); ok {
		if l.rf.killed() {
			return
		}
		if reply.Term != args.Term {
			l.stop()
			l.rf.mu.Lock()
			term := l.rf.Term
			if term < reply.Term {
				l.rf.convertToFollower(reply.Term)
			}
			l.rf.mu.Unlock()
		} else { // term is equal
			l.mu.Lock()
			defer l.mu.Unlock()
			if reply.Success {
				maxMatch := args.PrevLogIndex + len(args.Entries)
				if l.matchIndex[server] < maxMatch {
					l.matchIndex[server] = maxMatch
					l.nextIndex[server] = l.matchIndex[server] + 1
					l.c.tick()
				}
				//DPrintf("matchIndex : %d nextIndex : %d\n", maxMatch, maxMatch+1)
			} else {
				// term is equal, but previous log doesn't match
				if reply.ConflictTerm > args.PrevLogTerm {
					l.nextIndex[server] = reply.FirstIndex
					return
				}
				l.rf.mu.Lock()
				defer l.rf.mu.Unlock()
				if l.term != l.rf.Term {
					return
				}
				for index := args.PrevLogIndex; index >= 1; index-- {
					curEntry := l.rf.Logs[index-1]
					if curEntry.Term == reply.ConflictTerm {
						l.nextIndex[server] = index + 1
						return
					}
					if curEntry.Term < reply.ConflictTerm {
						l.nextIndex[server] = reply.FirstIndex
						return
					}
				}
			}
		}
	}
}

// committer periodically check match index of all server, if all match index is bigger than previous commit index, update the commit index then fetch
// new commit index.
type committer struct {
	tickChan chan struct{}
	l        *leadershipManager
}

func (c *committer) start() {
	l := c.l
	curMatch := 0
	for {
		select {
		case <-c.tickChan:
			// increase commit index as much as possible
			for {
				l.mu.Lock()
				matchCount := 0
				for _, x := range l.matchIndex {
					if x > curMatch {
						matchCount++
					}
				}
				l.mu.Unlock()
				if matchCount > len(l.matchIndex)/2 {
					curMatch++
					if curMatch <= l.commitIndex {
						continue
					}
				} else {
					break
				}
			}
			if curMatch > l.commitIndex {
				// we can commit something if possible
				l.rf.mu.Lock()
				// no one can modify commitIndex if we still in power
				if l.rf.state != Leader {
					l.rf.mu.Unlock()
					return
				}
				termToCommit := l.rf.Logs[curMatch-1].Term
				if termToCommit != l.term {
					l.rf.mu.Unlock()
					break
				}
				l.rf.commitIndex = curMatch
				l.commitIndex = curMatch
				l.rf.ap.signal()
				l.rf.mu.Unlock()
			}
		case <-l.rf.cancel:
			return
		}
	}
}

func (c *committer) tick() {
	select {
	case c.tickChan <- struct{}{}:
	default:
	}
}

//
func newLeadershipManager(rf *Raft) *leadershipManager {
	l := new(leadershipManager)
	l.term = rf.Term
	l.nextIndex = make([]int, len(rf.peers))
	l.matchIndex = make([]int, len(rf.peers))
	l.matchIndex[rf.me] = len(rf.Logs)
	l.rf = rf
	l.commitIndex = rf.commitIndex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		l.nextIndex[i] = len(rf.Logs) + 1
	}
	c := &committer{l: l, tickChan: make(chan struct{}, 1)}
	l.c = c
	go c.start()
	c.tick()
	return l
}
