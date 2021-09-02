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
	followersKilled := make(chan bool, rf.peerCnt - 1)
	rf.mu.Unlock()

	go func(){
			select{
				case <- heartsbeatsTimer.C:
					rf.wakeLeaderCond.Broadcast()  //wake up leader to send heartbeats
					heartsbeatsTimer.Reset(HEART_INTERVAL)
				case <- done:
					rf.logger.Log(raftlogs.DLeader, "S%d has stopped giving out heatbeats", rf.me)
					return

			}
		
			
		
	}()

}

						rf.logger.Log(raftlogs.DLeader, "S%d sending append entries to S%d", rf.me, peer)

