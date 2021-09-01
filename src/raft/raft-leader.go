package raft

import (
	"time"

	raftlogs "6.824/raft-logs"
)


func (rf *Raft) doLeaderWork(term int){
	rf.mu.Lock()
	if (rf.role != LEADER || rf.currentTerm != term){
		rf.logger.Log(raftlogs.DLeader, "S%d is no longer a leader and is stepping down", rf.me)
		rf.mu.Unlock()
		return
	}
	heartsbeatsTimer := time.NewTimer(HEART_INTERVAL)
	args := &AppendEntryArgs{
		Term: term,
		LeaderId: rf.me,
		PrevLogIndex: rf.lastLogIndex,
		PrevLogTerm: rf.logs[rf.lastLogIndex].Term,
	}	
	rf.mu.Unlock()
	go func(){
		for{
			
			select{
				case <- heartsbeatsTimer.C:
					rf.mu.Lock()
					if (rf.role != LEADER){
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					//need to send appendEntries to all followers
					rf.wakeLeaderCond.Broadcast()   //broadcast to wake the leader
					for i := 0; i < rf.peerCnt; i++{
						if i != rf.me{
							go func(peer int) {
								rf.logger.Log(raftlogs.DLeader, "S%d sending append entries to S%d", rf.me, peer)
								rf.sendAppendEntry(peer, args, &AppendEntryReply{})
							}(i)
						}
					}
					heartsbeatsTimer.Reset(HEART_INTERVAL)
				
			}
		}
			
		
	}()

}


