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

	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	logger "6.824/raft-logs"
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
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	snapshot []byte
	offset int   //so can access logs properly after recieving a snapshot

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.



	//volatile
	timerMuLock sync.Mutex
	timeToTimeOut time.Time
	logger logger.TopicLogger
	role int  //FOLLOWER = 0, CANDIDATE = 1, LEADER = 2
	highestCommitedIndex int
	lastApplied int

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

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}
type InstallSnapshotReply struct {
	Term int
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

func (rf *Raft) getRaftState() []byte{
	//get the current raft state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	//fmt.Println(tmp);
	e.Encode(rf.votedFor)
	e.Encode(rf.offset)
	//fmt.Println(tmp);
	e.Encode(rf.lastLogIndex)  //for the logs
	//fmt.Println(tmp);
	e.Encode(rf.logs)
	//fmt.Println(tmp);
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftState())
	//rf.logger.Log(raftlogs.DPersist, "S%d called persist", rf.me);
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset, lastLogIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&lastLogIndex) != nil{
		//rf.logger.Log(raftlogs.DError, "S%d failed in decoding the state 1", rf.me)
	} else {
		var logs []LogEntry = make([]LogEntry, lastLogIndex-rf.offset)
		if d.Decode(&logs) != nil{
			//rf.logger.Log(raftlogs.DError, "S%d failed in decoding the state 2", rf.me)
		}else{
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.lastLogIndex = lastLogIndex
			rf.logs = logs
			rf.offset = offset
			rf.highestCommitedIndex = rf.offset
			rf.lastApplied = rf.offset
			//rf.logger.Log(raftlogs.DLog, "S%d successsully restored its logs", rf.me)

		}

	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied >= lastIncludedIndex{
		return false
	}

	rf.doPersistAndSnap(lastIncludedIndex, lastIncludedTerm, snapshot)
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//server recieves a snapshot from leader
	defer func() {reply.Term = rf.currentTerm}()

	//rf.logger.Log(raftlogs.DSnap, "S%d term %d recv term %d installSnap:%d, myLast:%d\n",
		//rf.me, rf.currentTerm, args.Term, args.LastIncludedIndex, rf.lastLogIndex)
	
	if args.Term < rf.currentTerm{
		return
	}
	if args.Term > rf.currentTerm {
		rf.followerToFollowerWithHigherTermWithLock(args.Term)
	}
	rf.role = FOLLOWER
	rf.freshTimer()

	reply.Term = rf.currentTerm
	if rf.lastApplied >= args.LastIncludedIndex {

		//rf.logger.Log(raftlogs.DSnap, "S%d ignore install index %d for applied %d\n",
			// rf.me, rf.lastApplied, args.LastIncludedIndex)
		return
	}

	go func(){
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()


}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.logger.Log(raftlogs.DSnap, 
		// "S%d server recieved a snapshot from to index %d, and the logs will be from %d to %d",
		// rf.me, index, index + 1, rf.lastLogIndex)
	
	rf.doPersistAndSnap(index, rf.logs[index - rf.offset].Term, snapshot)

}

//hold lock
func (rf *Raft) doPersistAndSnap(index int, term int, snapshot []byte){

	rf.highestCommitedIndex = int(math.Max(float64(rf.highestCommitedIndex), float64(index)))
	rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(index)))

	lengthAfterTrimming := rf.lastLogIndex - index + 1

	if lengthAfterTrimming < 1 {
		rf.logs = make([]LogEntry, 1)
	}else{
		rf.logs = append([]LogEntry{}, rf.logs[index - rf.offset: ]...)
	}

	rf.lastLogIndex = int(math.Max(float64(index), float64(rf.lastLogIndex)))
	rf.logs[0].Term = term
	rf.logs[0].Command = nil


	rf.offset = index
	rf.snapshot = snapshot

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)

	//rf.logger.Log(raftlogs.DSnap, "S%d raft apply snapshot offset %d ,lastApplied %d,total log %d, size %d,log cap:%d\n",
		// rf.me, index, rf.lastApplied, rf.lastLogIndex, len(rf.getRaftState()), cap(rf.logs))
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
	NextIndex int //next
	RejectedByTerm bool

	//Optimization for decision of rollback in case of conflicts
	ConflictIndex int    //index of first entry in term of conflict
	ConflictTerm int      //index of term of conflict
	ConflictLen int   //length of whole l

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

	//rf.logger.Log(raftlogs.DVote, "S%d received a request vote from S%d, his term is %d and mine is %d", 
			// rf.me, args.CandidateId,  args.Term, rf.currentTerm)

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
		//rf.logger.Log(raftlogs.DTimer, 
			// "S%d is the leader and is requested to vote by another candidate S%d",
			//  rf.me, args.CandidateId)
			return
	}
	reply.Term = rf.currentTerm
	myIdx := rf.lastLogIndex
	myLastTerm := rf.logs[rf.lastLogIndex - rf.offset].Term
	if rf.votedFor == args.CandidateId || (
		rf.votedFor == -1 && checkCandidatesLogIsNew(myLastTerm, args.LastLogTerm,
											myIdx, args.LastLogIndex)){
		reply.VoteGranted = true
		//rf.logger.Log(raftlogs.DVote, 
			// "S%d granted its vote to candidate S%d and it currently is a %d", 
			// rf.me, args.CandidateId, rf.role)

		rf.freshTimer()
		rf.votedFor = args.CandidateId
		rf.persist()
	}else{
		reply.VoteGranted = false
		if rf.votedFor == -1 {
			//rf.logger.Log(raftlogs.DVote,
				//  "S%d rejected S%d because of failed log comparison",
				// rf.me, args.CandidateId)
		}else{
			//rf.logger.Log(raftlogs.DVote,
				// "S%d rejected S%d because already voted for S%d",
				// rf.me, args.CandidateId, rf.votedFor)
		}
	}


}


//hold lock
func (rf *Raft) appendOneLogEntry(logEntry LogEntry){
	rf.lastLogIndex++
	rf.logs = append(rf.logs, logEntry)
	//rf.logger.Log(raftlogs.DLog, "S%d appended log num %d to its logs", rf.me, rf.lastLogIndex)
	rf.persist()
}

func (rf *Raft) appendManyLogs(logEntries []LogEntry){
	rf.lastLogIndex += len(logEntries)
	rf.logs = append(rf.logs, logEntries...)
	//rf.logger.Log(raftlogs.DLog, "S%d appended from %d to %d", 
	// rf.me, rf.lastLogIndex - len(logEntries), rf.lastLogIndex)
	rf.persist()
}

func (rf *Raft) findTermsFirstIndex(from int) int {
	i := from - 1
	term := rf.logs[from-rf.offset].Term
	for i > rf.offset {
		if rf.logs[i-rf.offset].Term != term {
			break
		}
		i--
	}
	i++
	rf.AssertTrue(i >= rf.offset && rf.logs[i-rf.offset].Term == rf.logs[from-rf.offset].Term,
		"must equal,found i:%d Term:%d, from i:%d, Term:%d\n",
		i, rf.logs[i-rf.offset].Term, from, rf.logs[from-rf.offset].Term)
	return i
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {reply.Term = rf.currentTerm}()	

	leaderSendLastIndex := args.PrevLogIndex + len(args.Entries)

	if len(args.Entries) == 0 {
		//rf.logger.Log(raftlogs.DLog, "S%d with term %d recv term %d append from %d []\n", rf.me, rf.currentTerm,
			// args.Term, args.LeaderId)
	} else if len(args.Entries) == 1 {
		//rf.logger.Log(raftlogs.DLog, "S%d with term %d recv term %d append from %d [%d]\n", rf.me, rf.currentTerm,
			// args.Term, args.LeaderId,
			// args.PrevLogIndex+1)
	} else {
		//rf.logger.Log(raftlogs.DLog, "S%d with term %d recv term %d append from %d [%d->%d]\n", rf.me, rf.currentTerm,
			// args.Term, args.LeaderId,
			// args.PrevLogIndex+1, leaderSendLastIndex)
	}

	reply.Success = false
	reply.RejectedByTerm = false
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm{
		//rf.logger.Log(raftlogs.DLog, "S%d rejected request from S%d because lower term", rf.me, args.LeaderId)
		reply.RejectedByTerm = true
		return
	}
	defer rf.freshTimer()


	if args.Term > rf.currentTerm {
		term := args.Term
		rf.followerToFollowerWithHigherTermWithLock(term)
	}else{
		//I am a follower 
		rf.role = FOLLOWER
	}

	reply.NextIndex = leaderSendLastIndex + 1
	if args.PrevLogIndex > rf.lastLogIndex{
		//my logs are too short
		reply.ConflictTerm = -1
		reply.ConflictLen = rf.lastLogIndex + 1
		//rf.logger.Log(raftlogs.DLog, "S%d's logs are less than the leader's, %d is greater than my %d",
				// rf.me, args.PrevLogIndex, rf.lastLogIndex)
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if args.PrevLogIndex > rf.offset && rf.logs[args.PrevLogIndex-rf.offset].Term != args.PrevLogTerm {
		// when rejecting an AppendEntries request, the follower
		// can include the term of the conflicting entry and the first
		// index it stores for that term
		//rf.logger.Log(raftlogs.DLog, 
		//   "S%d has found a conflict#################  it rejects leader's S%d term %d pre[t%d, i%d], for last log's term: %d i: %d",
		//  rf.me, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogTerm, rf.logs[rf.lastLogIndex-rf.offset].Term, rf.lastLogIndex)
		
		//optimization mentioned in the paper
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-rf.offset].Term
		reply.ConflictLen = rf.lastLogIndex + 1
		reply.ConflictIndex = rf.findTermsFirstIndex(args.PrevLogIndex)
	
		return
	}


	reply.Success = true
	reply.NextIndex = leaderSendLastIndex + 1
	if leaderSendLastIndex <= rf.lastApplied {
		//rf.logger.Log(raftlogs.DLog, "S%d term %d log %d already applied", rf.me,
					// rf.currentTerm, leaderSendLastIndex)	
		return
	}

	//If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it

	scanFrom := args.PrevLogIndex + 1
	scanEnd := rf.lastLogIndex

	//already have alot of entries
	if scanEnd > leaderSendLastIndex {
		scanEnd = leaderSendLastIndex
	}
	if scanFrom <= rf.offset{
		scanFrom = rf.offset + 1
	}

	for scanFrom <= scanEnd {
		if rf.logs[scanFrom-rf.offset].Term != args.Entries[scanFrom - args.PrevLogIndex - 1].Term{
			//need to delete all tail logs
			rf.deleteTailLogs(scanFrom)
			break
		}
		scanFrom++
	}

	//Append any new entries not already in the log
	//append all the logs now and make sure to exclude those already appended
	if scanFrom <= leaderSendLastIndex{
		rf.appendManyLogs(args.Entries[scanFrom - args.PrevLogIndex - 1: ])
	}


	//If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry


	toCommit := args.LeaderCommit
	if toCommit > rf.lastLogIndex{
		toCommit = rf.lastLogIndex
	}
	if toCommit > rf.highestCommitedIndex {
		//need to commit 
		//rf.logger.Log(raftlogs.DCommit, "S%d about to commit from %d to %d",
		// rf.me, rf.highestCommitedIndex, toCommit)

		rf.highestCommitedIndex = toCommit
		if rf.highestCommitedIndex > rf.lastApplied{
			//need to apply
			rf.applyCond.Signal()
		}
	}


}


//hold lock
func (rf *Raft) deleteTailLogs(scanFrom int){
	rf.AssertTrue(scanFrom > 0 && scanFrom <= rf.lastLogIndex, 
				"from: %d lastLog: %d\n", scanFrom, rf.lastLogIndex)
	//rf.logger.Log(raftlogs.DLog, "S%d about to delete logs from %d to %d",
				// rf.me, scanFrom, rf.lastLogIndex)
				
	rf.logs = append([]LogEntry{}, rf.logs[:scanFrom - rf.offset]...)			
	rf.lastLogIndex = scanFrom - 1
	rf.persist()
}



func (rf *Raft) applyLogs(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed(){
		if rf.lastApplied < rf.highestCommitedIndex {
			length := rf.highestCommitedIndex - rf.lastApplied
			applyMsgs := make([]*ApplyMsg, length)


			for i := 0; i < length; i++ {
				rf.lastApplied++
				applyMsgs[i] = &ApplyMsg{
					CommandValid:  true,
					SnapshotValid: false,
					Command:       rf.logs[rf.lastApplied-rf.offset].Command,
					CommandIndex:  rf.lastApplied,
				}
				//rf.logger.Log(raftlogs.DLog, "S%d about to commit this %v at %v", rf.me, applyMsgs[i].Command, applyMsgs[i].CommandIndex);

			}

			rf.mu.Unlock()

			for _, msg := range applyMsgs {
				rf.applyCh <- *msg
			}

			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
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
	//rf.logger.Log(raftlogs.DClient, 
		// "S%d received a command %#v at index %d for the term %d", 
		// 	rf.me, command, rf.lastLogIndex + 1, rf.currentTerm)

	rf.appendOneLogEntry(LogEntry{
		Command: command,
		Term: term,
	})	
	index = rf.lastLogIndex

	rf.mu.Unlock()


	rf.wakeLeaderCond.Broadcast()
	
	return index, term, true
}

func (rf *Raft) followerToFollowerWithHigherTermWithLock(term int){
	//rf.logger.Log(raftlogs.DTimer, "S%d changed terms from %d to %d",
				// rf.me, rf.currentTerm, term)

	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
	rf.votes = 0
	rf.persist()
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
	//rf.logger.Log(raftlogs.DDrop, "S%d raft killed #######\n", rf.me)
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
			//rf.logger.Log(raftlogs.DDrop, "S%d has finally died", rf.me)
			rf.mu.Lock()
			killed = true
			//rf.logger.Log(raftlogs.DLog, "S%d logs len is %v", rf.me, len(rf.logs))
			rf.mu.Unlock()
	}()
	for {
			rf.mu.Lock()
			if killed {
				break
			}
			//rf.logger.Log(raftlogs.DDrop, "S%d not killed yet", rf.me)
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			if rf.checkTimeOut(){
				rf.freshTimer()
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
	labgob.Register(LogEntry{})
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peerCnt = len(peers)

    //rf.logger.Log(raftlogs.DTimer, "S%d just came to life ", rf.me);

	// Your initialization code here (2A, 2B, 2C).

	rf.mu = sync.Mutex{}
	rf.applyCh = applyCh
	rf.currentTerm = 0	
	rf.votedFor = -1
	rf.lastLogIndex = 0
	rf.highestCommitedIndex = 0
	rf.lastApplied = 0
	rf.logs = make([]LogEntry, 1)
	rf.offset = 0
	rf.snapshot = persister.ReadSnapshot()

	rf.killedChan = make(chan struct{})
	rf.majority = (len(peers) + 1) / 2;
	rf.votes = 0
	rf.role = FOLLOWER

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.wakeLeaderCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.initTimeOut()   //less than freshTimeout

	// initialize from state persisted before a crash
	// fmt.Println(persister.ReadRaftState())
	rf.readPersist(persister.ReadRaftState())
	//rf.logger.Log(raftlogs.DLog, "S%d logs len is %v", rf.me, len(rf.logs))

	//start a thread to wait for signals and applyLogs
	go rf.applyLogs()
	// start ticker goroutine to start elections
	go rf.ticker()

	 

	return rf
}

	