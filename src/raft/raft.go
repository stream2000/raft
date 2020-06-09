package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	BaseElectionTimeout = 400
	FloatTimeout        = 100
	HeartbeatInterval   = 50
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	state       raftState
	Term        int           // only make or function like convert to xxx can modify this
	me          int           // this peer's index into peers[]
	dead        int32         // set by Kill()
	cancel      chan struct{} // use to terminate all long run goroutines when the raft is killed
	VotedFor    int
	commitIndex int
	lastApplied int
	Logs        []LogEntry
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	timeout     *timeoutManager
	leadership  *leadershipManager
	election    *electionManager
	mu          sync.Mutex // Lock to protect shared access to this peer's state
	applyCh     chan ApplyMsg
	ap          *applyManager
}

type raftState int

const (
	Follower raftState = iota + 1
	Candidate
	Leader
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Term, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Logs)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Term)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var votedFor int
	var term int
	if d.Decode(&logs) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&term) != nil {
		panic("decode error")
	} else {
		rf.Logs = logs
		rf.VotedFor = votedFor
		rf.Term = term
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		index := -1
		term := -1
		return index, term, false
	}
	index := len(rf.Logs) + 1
	term := rf.Term
	// directly append the log then return, other background goroutine will check the length of logs
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.Logs = append(rf.Logs, entry)
	rf.persist()
	rf.leadership.matchIndex[rf.me] = len(rf.Logs)
	DPrintf("start: server %d add entry{%v} at term %d cur length %d\n", rf.me, command, rf.Term, len(rf.Logs))
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// close the channel so all long-run goroutine will know that the raft server is dead
	close(rf.cancel)
	DPrintf("kill server %d\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToCandidate() {
	DPrintf("server %d at term %d is converting to candidate at term %d\n", rf.me, rf.Term, rf.Term+1)
	rf.Term++
	rf.persist()
	rf.state = Candidate
	e := NewElectionManager(rf)
	rf.election = e
	args := &RequestVoteArgs{
		Term:         rf.Term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Logs),
	}
	if args.LastLogIndex != 0 {
		args.LastLogTerm = rf.Logs[args.LastLogIndex-1].Term
	}
	go e.send(args)
}

func (rf *Raft) convertToFollower(term int) {
	DPrintf("server %d at term %d is converting to follower at term %d\n", rf.me, rf.Term, rf.Term+1)
	rf.Term = term
	rf.persist()
	switch rf.state {
	case Follower:
		// is only maintaining the timeout state
		rf.timeout.restartTimer()
		return
	case Candidate:
		rf.election.cancel()
		rf.state = Follower
		rf.timeout.restartTimer()
		return
	case Leader:
		go rf.leadership.stop()
		rf.state = Follower
		timeout := NewTimeoutManager(rf, BaseElectionTimeout)
		rf.timeout = timeout
		go rf.timeout.start()
		return
	}
}

// from candidate to leader
func (rf *Raft) convertToLeader() {
	rf.state = Leader
	DPrintf("server %d at term %d is converting to leader \n", rf.me, rf.Term)
	go rf.timeout.stop()
	l := newLeadershipManager(rf)
	rf.leadership = l
	args := &AppendEntriesReq{
		Term:            l.term,
		LeaderId:        l.rf.me,
		LeaderCommitted: l.commitIndex,
	}
	next := len(rf.Logs) + 1
	if next != 1 {
		args.PrevLogIndex = next - 1
		args.PrevLogTerm = l.rf.Logs[next-2].Term
	}
	go rf.leadership.start(args)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// vote for nobody
	rf.VotedFor = -1
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.cancel = make(chan struct{})
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start the election timeout timer
	timeout := NewTimeoutManager(rf, BaseElectionTimeout)
	rf.timeout = timeout
	go rf.timeout.start()
	// command applier
	ap := newApplyManager(rf)
	rf.ap = ap
	go ap.start()
	// Your initialization code here (2A, 2B, 2C).

	return rf
}
