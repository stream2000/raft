/*
@Time : 2020/4/16 21:57
@Author : Minus4
*/
package raft

type applyManager struct {
	rf          *Raft
	indexChange chan struct{}
}

// newApplyManager periodically check the commitIndex and apply the log
// DEBUG no problem
func newApplyManager(rf *Raft) *applyManager {
	return &applyManager{rf: rf, indexChange: make(chan struct{}, 1)}
}

func (a *applyManager) start() {
	for {
		select {
		case <-a.rf.cancel:
			return
		case <-a.indexChange:
			// snapshot the commit index
			a.rf.mu.Lock()
			commitIndex := a.rf.commitIndex
			a.rf.mu.Unlock()
			for a.rf.lastApplied < commitIndex {
				a.rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      a.rf.logs[a.rf.lastApplied-1].Command,
					CommandIndex: a.rf.lastApplied,
				}
				DPrintf("server %d applied entry %v\n", a.rf.me, msg)
				a.rf.applyCh <- msg
			}
		}
	}
}

func (a *applyManager) signal() {
	select {
	case a.indexChange <- struct{}{}:
	default:
	}
}
