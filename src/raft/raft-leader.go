package raft

import (
	"sort"
	"time"

	raftlogs "6.824/raft-logs"
)


func (rf *Raft) doLeaderWork(leaderTerm int){
	rf.mu.Lock()
	if (rf.role != LEADER || rf.currentTerm != leaderTerm){     //difference
		rf.logger.Log(raftlogs.DLeader, "S%d is no longer a leader and is stepping down", rf.me)
		rf.mu.Unlock()
		return
	}
	heartsbeatsTimer := time.NewTimer(HEART_INTERVAL)
	followersKilled := make(chan bool, rf.peerCnt - 1)

	//start a thread that checks if it needs to wake up the leader or kill itself since it is no longer
	//the leader
	go func(){
		for !rf.killed(){
			select{
				case <- heartsbeatsTimer.C:
					rf.wakeLeaderCond.Broadcast()  //wake up leader to send heartbeats
					heartsbeatsTimer.Reset(HEART_INTERVAL)
				case <- followersKilled:
					rf.logger.Log(raftlogs.DLeader, "S%d has stopped giving out heatbeats", rf.me)
					rf.wakeLeaderCond.Broadcast()  //difference
					return

			}
		}
			
	}()

	rf.spawnPeerSyncers(leaderTerm, followersKilled)
	rf.mu.Unlock()
}

//hold lock
func (rf *Raft) spawnPeerSyncers(leaderTerm int, followersKilled chan bool){
	rf.logger.Log(raftlogs.DLeader, "S%d starting %d threads", rf.me, rf.peerCnt - 1)

	for i := 0; i < rf.peerCnt; i++{
		if i != rf.me {
			//start a go routine for each peer that keeps on asking them to commit
			tempInitialArgs := &AppendEntryArgs{
						Term: leaderTerm,
						LeaderId: rf.me,
						PrevLogIndex: rf.lastLogIndex,
						PrevLogTerm: rf.logs[rf.lastLogIndex].Term,
						Entries: rf.logs[0:0],
						LeaderCommit: rf.highestCommitedIndex,
					}
			go func(peer int){ 
				go rf.doAppendRPC(peer, leaderTerm, tempInitialArgs)
				rf.mu.Lock()
				defer func() {rf.logger.Log(raftlogs.DLeader, "S%d leader for term %d loop has ended", rf.me, leaderTerm)}()					
				defer rf.mu.Unlock()
				defer func(){
					followersKilled <- true    //signal to the leader that a follower has died
				}()

		
				//main loop for the peer
				for !rf.killed(){
					if (leaderTerm != rf.currentTerm || rf.role != LEADER){  //leader is no longer the leader
						return
					}

					//my last index is less than the peers next index, 
					//which means I have no logs to send
					if (rf.lastLogIndex < rf.nextIndex[peer]){ 
						rf.logger.Log(raftlogs.DLeader, "S%d has no entries to send, so only sending hearbeats", rf.me)
						//send a heartbeat
						prevLogIndex := rf.nextIndex[peer] - 1
						prevLogTerm := rf.logs[prevLogIndex].Term
						if prevLogIndex != rf.lastLogIndex{
							rf.logger.Log(raftlogs.DLeader, 
								"S%d has a problem his prevLogIdx is %d and mine is %d",
								rf.me, prevLogIndex, rf.lastLogIndex)
						}

						go rf.doAppendRPC(peer, leaderTerm, &AppendEntryArgs{
							Term: leaderTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
							Entries: []LogEntry{}, LeaderCommit: rf.highestCommitedIndex,
						})

					}else{
						//need to send actual logs

						rf.AssertTrue(rf.nextIndex[peer] > rf.matchIndex[peer], "term %d for S%d next %d match %d\n",
							leaderTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])

						prevLogIndex := rf.nextIndex[peer] - 1
						prevLogTerm := rf.logs[prevLogIndex].Term
						args := &AppendEntryArgs{
							Term: leaderTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
							Entries: rf.logs[prevLogIndex + 1: ], LeaderCommit: rf.highestCommitedIndex,
						}
						go rf.doAppendRPC(peer, leaderTerm, args)
					}
					
					// rf.logger.Log(raftlogs.DLeader, "S%d sending append entries to S%d", rf.me, peer)

					//sleep until leader wakes up to send an append entry
					rf.wakeLeaderCond.Wait()
				}
				
			}(i)
		}
	}
}


func (rf *Raft) doAppendRPC(peer int, term int, args *AppendEntryArgs){
	if len(args.Entries) == 0 {
		rf.logger.Log(raftlogs.DLeader, "S%d sent an append entry as a heartbeat to S%d", 
		rf.me, peer)
	}else if len(args.Entries) == 1 {
		rf.logger.Log(raftlogs.DLeader, "S%d sent an append entry with only one piece of data data %v to S%d", 
		rf.me, args.Entries, peer)
	}else{
		rf.logger.Log(raftlogs.DLeader, "S%d sent an append entry with multiple pieces of data %v to S%d", 
		rf.me, args.Entries, peer)
	}

	reply := &AppendEntryReply{
		Term: 0,
		Success: false,
		NextIndex: 1,
	}

	ok := rf.sendAppendEntry(peer, args, reply)

	if (!ok || rf.killed()){
		return
	}
	
	
	//check if the reply is valid and whether I shouldnt stay a leader
	rf.checkAppendEntryReplyRPC(peer, term, reply)
	
}
func getKth(c []int, k int) int {
	sort.Ints(c)
	return c[k]
}
func (rf *Raft) appendOkAsLeader(nextIndex, peer int, isAppend bool) {
	// if isAppend {
	// 	rf.logger.L(logger.Leader, "term %d leader append ok to S%d,nextIndex %d\n",
	// 		rf.currentTerm, peer, nextIndex)
	// } else {
	// 	rf.logger.L(logger.Leader, "term %d leader install ok to S%d,nextIndex %d\n",
	// 		rf.currentTerm, peer, nextIndex)
	// }

	rf.AssertTrue(rf.nextIndex[peer] > rf.matchIndex[peer], "term %d for S%d next %d match %d\n",
		rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])

	// if rf.matchIndex[peer] > nextIndex-1 {
	// 	rf.logger.L(logger.Leader, "term %d leader reject append ok: next %d, match %d\n ",
	// 		rf.currentTerm, nextIndex, rf.matchIndex[peer])
	// 	return
	// } else if rf.matchIndex[peer] == nextIndex-1 {
	// 	rf.logger.L(logger.Leader, "term %d leader recv S%d heart %d[] reply\n",
	// 		rf.currentTerm, peer, nextIndex-1)
	// }

	rf.nextIndex[peer] = nextIndex
	rf.matchIndex[peer] = nextIndex - 1

	//find major match: the maximum index that there are majority raft peers
	//whose matchIndex equals or greater than index
	kth := rf.peerCnt / 2
	to_sort := make([]int, rf.peerCnt)
	copy(to_sort, rf.matchIndex)
	to_sort[rf.me] = rf.lastLogIndex
	major_match := getKth(to_sort, kth)

	if major_match > rf.lastLogIndex {

		// rf.logger.L(logger.Leader, "term:%d , major_match:%d  > lastLogIndex:%d, S%d  matchIndex:%d nextIndex:%d->%d\n",
		// 	rf.currentTerm, major_match, rf.lastLogIndex, peer, rf.matchIndex,
		// 	rf.nextIndex, nextIndex)

		panic("leader")
	}

	//try to commit
	if major_match > rf.highestCommitedIndex &&
		rf.logs[major_match].Term == rf.currentTerm {

		// if major_match == rf.highestCommitedIndex+1 {
		// 	rf.logger.L(logger.Commit, "term %d leader commit [%d]\n",
		// 		rf.currentTerm, major_match)
		// } else {
		// 	rf.logger.L(logger.Commit, "term %d leader commit [%d->%d]\n",
		// 		rf.currentTerm, rf.highestCommitedIndex+1, major_match)
		// }

		rf.highestCommitedIndex = major_match
		rf.applyCond.Signal()
	}
}

func (rf *Raft) checkAppendEntryReplyRPC(peer int, term int, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Log(raftlogs.DLeader, "S%d got a reply from %d", rf.me, peer)

	//i have a lower term number and shouldnt be leader
	if reply.Term > rf.currentTerm {
		rf.followerToFollowerWithHigherTermWithLock(reply.Term)
		return
	}

	if rf.currentTerm != term || 
		rf.role != LEADER || reply.Term < rf.currentTerm || reply.RejectedByTerm {
		return
	}

	//succesful appendEntry
	if reply.Success {
		rf.appendOkAsLeader(reply.NextIndex, peer, true)
		return
	}


	//there is a conflict so need to decrement index
	// With this information, the
	// leader can decrement nextIndex to bypass all of the conflicting entries in that term; one AppendEntries RPC will
	// be required for each term with conflicting entries, rather
	// than one RPC per entry. In practice, we doubt this optimization is necessary, since failures happen infrequently
	// and it is unlikely that there will be many inconsistent entries

	//calculate where to rollback
	nextIndex := rf.getNextIndex(reply.ConflictTerm, reply.ConflictIndex, reply.ConflictLen)
	rf.logger.Log(raftlogs.DLeader, "S%d about to initiate logs rollback for S%d from %d to %d", 
			rf.me, peer, rf.nextIndex[peer], nextIndex)

	//check if outdated message
	if rf.matchIndex[peer] >= nextIndex {
		rf.logger.Log(raftlogs.DLeader, "S%d the leader rejects append conflict, next %d but match is %d",
						rf.me, nextIndex, rf.matchIndex[peer])
		return	
	}

	rf.nextIndex[peer] = nextIndex
	rf.logger.Log(raftlogs.DLeader, "S%d updated D%d nextIdx to %d",
		rf.me, peer, nextIndex)

}

func (rf *Raft) getNextIndex(conflictTerm, conflictIndex, conflictLen int) int {
	if conflictTerm == -1{
		//case where logs are too short
		return conflictLen
	}

	has, index := rf.hasTermAndLastIndex(conflictTerm)
	if !has{
		return conflictIndex
	}
	return index
}

//hold lock
//check if leader has logs with conflictTerm and return true and the first index of the logs with this term
//else return false

func (rf *Raft) hasTermAndLastIndex(conflictTerm int) (bool, int) {
	i := rf.lastLogIndex
	has := false

	for i > 0 {
		if rf.logs[i].Term == conflictTerm{
			has = true
		}else if rf.logs[i].Term < conflictTerm {
			break
		}
		i--
	}

	if !has {
		return false, -1
	}
	return true, i + 1 
}