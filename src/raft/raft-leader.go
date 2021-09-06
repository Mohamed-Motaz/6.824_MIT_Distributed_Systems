package raft

import (
	"time"

	raftlogs "6.824/raft-logs"
)


func (rf *Raft) doLeaderWork(leaderTerm int){
	rf.mu.Lock()
	if (rf.role != LEADER || rf.currentTerm != leaderTerm){
		rf.logger.Log(raftlogs.DLeader, "S%d is no longer a leader and is stepping down", rf.me)
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
					return

			}
		}
			
	}()

	rf.spawnPeerSyncers(leaderTerm, followersKilled)
	rf.mu.Unlock()
}

func (rf *Raft) spawnPeerSyncers(leaderTerm int, followersKilled chan bool){
	rf.logger.Log(raftlogs.DLeader, "S%d starting %d threads", rf.me, rf.peerCnt - 1)

	for i := 0; i < rf.peerCnt; i++{
		if i != rf.me {
			//start a go routine for each peer that keeps on asking them to commit
			go func(peer int){ 

				go func(){
					rf.mu.Lock()
					//send an initial heartbeat to tell all peers I am leader
					tempInitialArgs := &AppendEntryArgs{
						Term: leaderTerm,
						LeaderId: rf.me,
						PrevLogIndex: rf.lastLogIndex,
						PrevLogTerm: rf.logs[rf.lastLogIndex].Term,
						Entries: []LogEntry{},
						LeaderCommit: rf.highestCommitedIndex,
					}
					rf.mu.Unlock()

					rf.doAppendRPC(peer, leaderTerm, tempInitialArgs)
				}()
					
				defer func(){
					followersKilled <- true    //signal to the leader that a follower has died
				}()
				rf.mu.Lock()
				defer rf.mu.Unlock()
		
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
						prevLogIndex := rf.nextIndex[peer] - 1
						prevLogTerm := rf.logs[prevLogIndex].Term
						args := &AppendEntryArgs{
							Term: leaderTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
							Entries: rf.logs[prevLogIndex + 1: ], LeaderCommit: rf.highestCommitedIndex,
						}
						go rf.doAppendRPC(peer, leaderTerm, args)
					}
					
					rf.logger.Log(raftlogs.DLeader, "S%d sending append entries to S%d", rf.me, peer)

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

func (rf *Raft) checkAppendEntryReplyRPC(peer int, term int, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
}
