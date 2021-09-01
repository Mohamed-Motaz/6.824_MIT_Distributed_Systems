package raft

import (
	raftlogs "6.824/raft-logs"
)

func (rf *Raft) doWorkBeforeTransitioningToLeader(){
	rf.role = LEADER
	rf.logger.Log(raftlogs.DLeader, 
		"S%d just become leader for the term %d",
		rf.me, rf.currentTerm)
	for i := 0; i < rf.peerCnt; i++{
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) doLeaderWork(term int, args *RequestVoteArgs){
	for i := 0; i < rf.peerCnt; i++ {
		if i != rf.me {
			rf.logger.Log(raftlogs.DTimer, "S%d sent a request vote to S%d", rf.me, i);
			go func(peer int) {
				reply := &RequestVoteReply{
					Term: -1,
					VoteGranted: false,
				}
				ok := rf.sendRequestVote(peer, args, reply)
				
				if ok && reply.Term > term{
					rf.candidateToFollowerBecauseOfHigherTerm(reply.Term)
					return
				}

				if rf.role != CANDIDATE || //i am no longer a candidate
					!ok ||   //failed to make rpc call
					!reply.VoteGranted{   //vote not granted 
					return;
				} 
				//rf.mu.Lock()
				rf.votes++;
				rf.logger.Log(raftlogs.DVote, "S%d got a vote from S%d", rf.me, peer)
				if rf.votes >= rf.majority{
					rf.doWorkBeforeTransitioningToLeader()
					go rf.doLeaderWork()
				}
				//rf.mu.Unlock()
				
			}(i)
		}
	}
}


func (rf *Raft) newElection(){
	rf.logger.Log(raftlogs.DTimer, "S%d is starting a new election", rf.me);

	rf.mu.Lock()
	if rf.role == LEADER {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.votes = 1
	rf.mu.Unlock()
	
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm: rf.logs[rf.lastLogIndex].Term,
	}
	rf.doCandidateWork(rf.currentTerm, args)


}