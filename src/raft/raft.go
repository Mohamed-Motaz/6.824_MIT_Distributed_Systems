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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	raftlogs "6.824/raft-logs"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	ELECTION_TIMEOUT = 800 * time.Millisecond
	RANDOM_PLUS      = 200 * time.Millisecond
	HEART_INTERVAL   = 300 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct{
	Term int              //term when entry is recieved by leader, first idx is 1
	Command interface{}   //can be of any type
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	peerCnt   int
	wakeLeaderCond *sync.Cond
	killedChan chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.



	//volatile
	timerMuLock sync.Mutex
	timeToTimeOut time.Time
	logger raftlogs.Logger
	role int  //FOLLOWER = 0, CANDIDATE = 1, LEADER = 2
	majority int
	votes int

	//volatile for leader's every election, so re-init at every election
	nextIndex []int   //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int  //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//persisted
	currentTerm int     //latest term server has seen  0 by default
	votedFor int       //candidateId that received vote in current term, -1 by default
	logs []LogEntry   
	lastLogIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	var term int = rf.currentTerm
	var isleader bool = rf.role == LEADER
	rf.mu.Unlock()

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int  //candidate's term
	CandidateId int //candidate requesting the vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntryArgs struct {
	Term int //currentTerm of leader
	LeaderId int 
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntryReply struct {
	// Your data here (2A).
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//I am asked if I agree for the candidate to become a leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func(){
		reply.Term = rf.currentTerm     //always ensure the term gets updated before the function returns
	}()

	rf.logger.Log(raftlogs.DVote, "S%d received a request vote from S%d, his term is %d and mine is %d", 
			rf.me, args.CandidateId,  args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		//his term is higher than mine
		rf.followerToFollowerWithHigherTermWithLock(args.Term)	

	}
	if rf.role == LEADER {
		//I am a leader
		reply.VoteGranted = false
		rf.logger.Log(raftlogs.DTimer, 
			"S%d is the leader and is requested to vote by another candidate S%d",
			 rf.me, args.CandidateId)
			return
	}
	myIdx := rf.lastLogIndex
	myLastTerm := rf.logs[rf.lastLogIndex].Term
	if rf.votedFor == args.CandidateId || (
		rf.votedFor == -1 && checkCandidatesLogIsNew(myLastTerm, args.LastLogTerm,
											myIdx, args.LastLogIndex)){
		reply.VoteGranted = true
		rf.logger.Log(raftlogs.DVote, 
			"S%d granted its vote to candidate S%d and it currently is a %d", 
			rf.me, args.CandidateId, rf.role)

		rf.initTimeOut()
		rf.votedFor = args.CandidateId
	}else{
		reply.VoteGranted = false
		if rf.votedFor == -1 {
			rf.logger.Log(raftlogs.DVote,
				 "S%d rejected S%d because of failed log comparison",
				rf.me, args.CandidateId)
		}else{
			rf.logger.Log(raftlogs.DVote,
				"S%d rejected S%d because already voted for S%d",
				rf.me, args.CandidateId, rf.votedFor)
		}
	}


}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply){
	rf.logger.Log(raftlogs.DInfo, "S%d received appendEntry from leader S%d",
				rf.me, args.LeaderId)
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.logger.Log(raftlogs.DInfo, 
			"S%d is no longer a leader for the term %d, since S%d is now the leader for the term %d",
		rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.followerToFollowerWithHigherTermWithLock(args.Term)
	}
	rf.mu.Unlock()

	rf.initTimeOut()
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
	index := -1
	term := -1	
	isLeader := true
	
	rf.mu.Lock()

	term = rf.currentTerm
	isLeader = rf.role == LEADER
	if !isLeader || rf.killed(){
		rf.mu.Unlock()
		return 0, term, false
	}
	rf.logger.Log(raftlogs.DClient, 
		"S%d received a command %#v at index %d for the term %d", 
			rf.me, command, rf.lastLogIndex + 1, rf.currentTerm)

	rf.appendOneLogEntry(LogEntry{
		Command: command,
		Term: term,
	})

	rf.mu.Unlock()

	return index, term, isLeader
}

//hold lock
func (rf *Raft) appendOneLogEntry(logEntry LogEntry){
	rf.lastLogIndex++
	rf.logs = append(rf.logs, logEntry)
	rf.logger.Log(raftlogs.DLog, "S%d appended log num %d to its logs", rf.me, rf.lastLogIndex)
}

func (rf *Raft) followerToFollowerWithHigherTermWithLock(term int){
	rf.logger.Log(raftlogs.DTimer, "S%d changed terms from %d to %d",
				rf.me, rf.currentTerm, term)

	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
	rf.votes = 0

}

func (rf *Raft) candidateToFollowerBecauseOfHigherTermWithLock(term int){
	//called when the candidate discovers a higher term follower
	rf.logger.Log(raftlogs.DTerm, 
		"S%d term change from %d to %d as he was demoted from candidate to follower",
		 rf.me, rf.currentTerm, term)

	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.votes = 0
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
	// Your code here, if desired.
	rf.logger.Log(raftlogs.DDrop, "S%d raft killed #######\n", rf.me)
	rf.killedChan <- struct{}{}

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	killed := false
	go func(){    //create a go routine to constantly check if I was killed or not
			<- rf.killedChan    //block until u receive killed from the channel
			rf.logger.Log(raftlogs.DDrop, "S%d has finally died", rf.me)
			rf.mu.Lock()
			killed = true
			rf.mu.Unlock()
	}()
	for {
			rf.mu.Lock()
			if killed {
				break
			}
			rf.logger.Log(raftlogs.DDrop, "S%d not killed yet", rf.me)
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			if rf.checkTimeOut(){
				rf.initTimeOut()
				if rf.role != LEADER{
					go rf.newElection()

				}
			}			
			rf.mu.Unlock()

			rf.sleepTimeOut()
		
	}
		
	

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

    rf.logger.Log(raftlogs.DTimer, "S%d just came to life ", rf.me);
	// Your initialization code here (2A, 2B, 2C).

	rf.mu = sync.Mutex{}
	rf.currentTerm = 0	
	rf.peerCnt = len(peers)
	rf.votedFor = -1
	rf.lastLogIndex = 0
	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.killedChan = make(chan struct{})
	rf.majority = (len(peers) + 1) / 2;
	rf.votes = 0
	rf.role = FOLLOWER
	rf.wakeLeaderCond = sync.NewCond(&rf.mu)
	rf.initTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

	