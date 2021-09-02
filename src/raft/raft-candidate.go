package raft

import (
	raftlogs "6.824/raft-logs"
)

func (rf *Raft) doWorkBeforeTransitioningToLeader(){
	rf.role = LEADER
	rf.logger.Log(raftlogs.DLeader, 
		"S%d just become leader for the term %d",
		rf.me, rf.currentTerm)
	for i := 0; i < rf.peerCnt && !rf.killed(); i++{
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) doCandidateWork(term int, args *RequestVoteArgs){
	for i := 0; i < rf.peerCnt && !rf.killed(); i++ {
		if i != rf.me {
			go func(peer int) {
				reply := &RequestVoteReply{
					Term: -1,
					VoteGranted: false,
				}
				rf.logger.Log(raftlogs.DVote, "S%d sent a request vote to S%d", rf.me, peer);
				ok := rf.sendRequestVote(peer, args, reply)				
				rf.logger.Log(raftlogs.DVote, "S%d got this reply from S%d", rf.me, peer)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Term > rf.currentTerm{
					rf.candidateToFollowerBecauseOfHigherTermWithLock(reply.Term)
					return
				}

				if rf.role != CANDIDATE || //i am no longer a candidate
					rf.currentTerm != term || //myCurrentTerm changed while I was a candidate
					!ok ||   //failed to make rpc call
					!reply.VoteGranted {   //vote not granted 
						

					return;
				} 
				rf.votes++;
				rf.logger.Log(raftlogs.DVote, "S%d got a vote from S%d", rf.me, peer)
				if rf.votes >= rf.majority{
					rf.logger.Log(raftlogs.DLeader, "S%d just won the election", rf.me)
					rf.doWorkBeforeTransitioningToLeader()
					go rf.doLeaderWork(rf.currentTerm)
				}
				
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
	rf.votedFor = rf.me
	rf.role = CANDIDATE
	term  := rf.currentTerm
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm: rf.logs[rf.lastLogIndex].Term,
	}	
	rf.mu.Unlock()

	rf.doCandidateWork(term, args)


}