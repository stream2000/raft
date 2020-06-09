/*
@Time : 2020/4/13 09:40
@Author : Minus4
*/
package raft

import (
	"sync"
	"sync/atomic"
)

// electionManager manages events when raft becomes candidate
// The election manager will periodically send RequestVote Rpc to all other servers
// until it gains enough votes or is canceled by other events
type electionManager struct {
	rf        *Raft
	voteCount int
	mu        sync.Mutex
	finished  int32
}

// send recursively execute until this election process is finished
func (e *electionManager) send(args *RequestVoteArgs) {
	for i := range e.rf.peers {
		if i == e.rf.me {
			continue
		}
		go e.requestVoteHandler(i, args)
	}
}

// requestVoteHandler will do  request vote  and process the reply
func (e *electionManager) requestVoteHandler(server int, args *RequestVoteArgs) {
	reply := new(RequestVoteReply)
	if ok := e.rf.sendRequestVote(server, args, reply); ok {
		// 可能在别处改变了term,
		if atomic.LoadInt32(&e.finished) == 1 {
			return
		}
		if reply.Term != args.Term {
			e.cancel()
			e.rf.mu.Lock()
			// get current term and compare
			term := e.rf.Term
			if term < reply.Term {
				e.rf.convertToFollower(reply.Term)
			}
			e.rf.mu.Unlock()
			return
		} else {
			if reply.VoteGranted {
				//				DPrintf("server %d grant vote to %d at term %d\n", server, args.CandidateId, args.Term)
				e.mu.Lock()
				defer e.mu.Unlock()
				e.voteCount++
				// the candidate have received vote from a majority server
				if e.voteCount > len(e.rf.peers)/2 {
					if atomic.LoadInt32(&e.finished) == 1 {
						return
					} else {
						// end election
						e.cancel()
					}
					e.rf.mu.Lock()
					defer e.rf.mu.Unlock()
					if e.rf.state == Candidate && e.rf.Term == args.Term {
						e.rf.convertToLeader()
					}
				}
			} else {
				// be rejected, sleep
				DPrintf("server %d was rejected by %d\n", e.rf.me, server)
				e.rf.timeout.restartTimer()
			}
		}
	}
	return
}

func (e *electionManager) cancel() {
	atomic.StoreInt32(&e.finished, 1)
}

func NewElectionManager(rf *Raft) *electionManager {
	e := &electionManager{
		rf:        rf,
		voteCount: 1,
	}
	return e
}
