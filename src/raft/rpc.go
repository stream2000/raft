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

// DEBUG 2B no problem
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}
	// do up-to-date check here before check other conditions
	moreUpToDate := true
	reply.Term = args.Term
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}
	if lastLogTerm > args.LastLogTerm {
		moreUpToDate = false
	}
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		moreUpToDate = false
	}

	if rf.term < args.Term {
		rf.convertToFollower(args.Term)
		if moreUpToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			rf.votedFor = -1
			reply.VoteGranted = false
		}
		return
	}
	// the follower changes it's term
	if rf.term == args.Term {
		if rf.state == Follower && rf.votedFor == -1 && moreUpToDate {
			rf.timeout.restartTimer()
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
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
	Entries         []LogEntry
	LeaderCommitted int
}

type AppendEntriesResp struct {
	Term         int
	Success      bool
	ConflictTerm int
	FirstIndex   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesResp) {
	rf.mu.Lock()
	//DPrintf("receive heartbeat from %d at term %d\n", args.LeaderId, args.Term)
	defer rf.mu.Unlock()

	// #1 term check
	if rf.term > args.Term {
		// reject this request
		reply.Term = rf.term
		return
	}
	defer rf.timeout.restartTimer()
	reply.Term = args.Term
	if rf.state == Candidate || rf.term < args.Term {
		rf.convertToFollower(args.Term)
	} // else, in the same term, and the server already know the leader

	// #2 check weather previous log term matched, and do the optimization mentioned in page 7-8
	prevLogTerm := 0
	if args.PrevLogIndex > len(rf.logs) {
		// make reply.ConflictTerm  bigger than args.Term so that leader can adopt the first index
		reply.ConflictTerm = -1
		reply.FirstIndex = len(rf.logs)
		return
	}
	if args.PrevLogIndex != 0 {
		prevLogTerm = rf.logs[args.PrevLogIndex-1].Term
	}
	// log conflicts, cal the firstIndex of the conflicting term in logs
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		first := args.PrevLogIndex
		for first >= 1 {
			if rf.logs[first-1].Term == prevLogTerm {
				reply.FirstIndex = first
				first--
			} else {
				break
			}
		}
		return
	}

	// after match check, the AppendEntries request will be success, and we can update the commit index if necessary
	reply.Success = true
	// #5 check commit index in defer function
	indexOfLastNewEntry := args.PrevLogIndex
	defer func() {
		mc := min(args.LeaderCommitted, indexOfLastNewEntry)
		if mc > rf.commitIndex {
			rf.commitIndex = mc
			rf.ap.signal()
		}
	}()

	// #3 new entry conflicting check
	if len(args.Entries) == 0 { // heartbeat message, return true
		return
	}
	if args.PrevLogIndex == len(rf.logs) { // nil entry won't conflict with the incoming entry
		reply.Success = true
		rf.logs = append(rf.logs, args.Entries...)
		indexOfLastNewEntry = len(rf.logs)
		return
	}

	for index := 0; index < len(args.Entries); index++ {
		if index+args.PrevLogIndex+1 <= len(rf.logs) {
			if args.Entries[index].Term != rf.logs[index+args.PrevLogIndex].Term { // conflicts with new entry
				// truncate the logs
				rf.logs = rf.logs[:index+args.PrevLogIndex]
				rf.logs = append(rf.logs, args.Entries[index:]...)
				indexOfLastNewEntry = len(rf.logs)
				return
			}
		} else {
			rf.logs = append(rf.logs, args.Entries[index:]...)
			indexOfLastNewEntry = len(rf.logs)
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesResp) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
