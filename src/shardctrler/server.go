package shardctrler

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead int32
	maxraftstate int //snapshot if log grows this big
	persister *raft.Persister

	next_seq map[int64]int  //for each client, his next seq for duplicate supression
	lastApplied int

	reply_chan map[int]chan bool
	logger     logger.TopicLogger
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}


// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	labgob.Register(Config{})
	labgob.Register(JoinArgs{})
	labgob.Register(GIDandShard{})
	
	sc := &ShardCtrler{
		me:           me,
		configs:      make([]Config, 1),
		maxraftstate: -1,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg, 30),
		reply_chan:   make(map[int]chan bool),
		lastApplied:  0,
		next_seq:     make(map[int64]int),
		logger: logger.TopicLogger{
			Me: me,
		},
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.configs[0].Groups = map[int][]string{}
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.applyInstallSnapshot(snap)

	gp sc.applier()
	// Your code here.

	return sc
}
