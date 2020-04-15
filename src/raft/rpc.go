/*
@Time : 2020/4/16 00:05
@Author : Minus4
*/
package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}
	// TODO do up-to-date check here before check other conditions
	reply.Term = args.Term
	// unconditionally convert to follower
	if rf.term < args.Term {
		rf.votedFor = args.CandidateId
		rf.convertToFollower(args.Term)
		reply.VoteGranted = true
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesReq struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entry           LogEntry
	LeaderCommitted int
}

type AppendEntriesResp struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesResp) {
	// TODO optimize the bool logic in Lab2B and Lab2C
	rf.mu.Lock()
	//DPrintf("receive heartbeat from %d at term %d\n", args.LeaderId, args.Term)
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		// reject this request
		reply.Term = rf.term
		return
	} else if args.Term == rf.term {
		// TODO do more things and check in Lab2B and Lab2C
		rf.timeout.restartTimer()
		reply.Success = true
		reply.Term = args.Term
		if rf.state == Follower {
			// Heartbeat or something else
			return
		}
		rf.convertToFollower(args.Term)
		return
	} else {
		rf.timeout.restartTimer()
		reply.Success = true
		reply.Term = args.Term
		rf.convertToFollower(args.Term)
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesResp) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
