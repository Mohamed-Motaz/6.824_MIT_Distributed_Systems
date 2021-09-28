package shardctrler

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
	raftlogs "6.824/raft-logs"
)



type Command struct {
	// Your data here.
	ClientId int64
	Seq int
	OptType int
	Opt interface{}
}

const (
	Debug     = false
	TYPE_JOIN = iota
	TYPE_LEAVE
	TYPE_MOVE
	TYPE_QUERY
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

func (sv *ShardCtrler) getReplyStruct(optType int, wrongLeader bool) interface{} {
	switch optType {
	case TYPE_JOIN:
		return &JoinReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_LEAVE:
		return &LeaveReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_MOVE:
		return &MoveReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_QUERY:
		return &QueryReply{
			WrongLeader: wrongLeader,
		}

	default:
		return nil
	}
}

//return reference type
func (sc *ShardCtrler) doRequest(command *Command) interface{} {
	sc.mu.Lock()
	sc.logger.L(logger.CtrlerReq, "do request args:%#v \n", command)
	
	//check if already applied
	if ok, res := sc.hasResult(command); ok{
		sc.mu.Unlock()
		return res
	}

	index, _, isLeader := sc.Raft().Start(*command)

	if !isLeader {
		sc.logger.L(logger.CtrlerReq, "declined [%3d--%d] for not leader\n",
			command.ClientId%1000, command.Seq)
		sc.mu.Unlock()
		return sc.getReplyStruct(command.OptType, true)
	}else {
		sc.logger.L(logger.CtrlerStart, "start [%3d--%d] as leader?\n",
			command.ClientId%1000, command.Seq)
	}

	wait_ch := sc.getWaitChan(index)
	sc.mu.Unlock()
	timeout := time.NewTimer(time.Millisecond * 200)
	
	select {
	case <- wait_ch:
	case <- timeout.C:
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if ok, res := sc.hasResult(command); ok{
		sc.logger.L(logger.CtrlerReq, "[%3d--%d] success !!!!! \n",
			command.ClientId%1000, command.Seq)
		return res
	}else {

		sc.logger.L(logger.CtrlerReq, "[%3d--%d] failed applied \n",
			command.ClientId%1000, command.Seq)
		return sc.getReplyStruct(command.OptType, true)
	}

}

//hold lock
func (sc *ShardCtrler) hasResult(req *Command) (bool, interface{}) {
	if sc.next_seq[req.ClientId] > req.Seq {
		res := sc.getReplyStruct(req.OptType, false)
		if (req.OptType == TYPE_QUERY){
			reply := res.(*QueryReply)
			reply.Config = *sc.queryConfig(req.Opt.(int))
			return true, reply
		}
		return true, res
	}

	return false, nil
}

//hold lock
func (sc *ShardCtrler) getWaitChan(index int) chan bool{
	if _, ok := sc.reply_chan[index]; !ok{
		sc.reply_chan[index] = make(chan bool)
	}
	return sc.reply_chan[index]
}
func (sc *ShardCtrler) notify(index int) {
	if c, ok := sc.reply_chan[index]; ok {
		close(c)
		delete(sc.reply_chan, index)
	}
}

//recieve messages on the applyCh
func (sc *ShardCtrler) applier(){
	for !sc.killed(){
		mes := <- sc.applyCh
		sc.mu.Lock()
		if mes.SnapshotValid { //raft needs a snapshot
			sc.logger.L(logger.CtrlerSnap, "ctrl recv Installsnapshot %v %v\n", mes.SnapshotIndex, sc.lastApplied)
			if sc.rf.CondInstallSnapshot(mes.SnapshotTerm,
				mes.SnapshotIndex, mes.Snapshot) {
				old_apply := sc.lastApplied
				sc.logger.L(logger.CtrlerSnap, "ctrl decide Installsnapshot %v <- %v\n", mes.SnapshotIndex, sc.lastApplied)
				sc.applyInstallSnapshot(mes.Snapshot)
				for i := old_apply + 1; i <= mes.SnapshotIndex; i++ {
					sc.notify(i)   //delete all reply_chan for this seq of indices
				}
			}
		}else if mes.CommandValid && mes.CommandIndex == 1 + sc.lastApplied{
			sc.logger.L(logger.CtrlerApply, "apply %d %#v lastApplied %v\n", mes.CommandIndex, mes.Command, sc.lastApplied)

			sc.lastApplied = mes.CommandIndex
			v, ok := mes.Command.(Command)
			if !ok{
				panic("")
			}
			sc.applyCommand(v)
			//correct command index so must do it here
			if sc.raftNeedSnapshot() {
				sc.doSnapshotForRaft(mes.CommandIndex)
			}
	
			sc.notify(mes.CommandIndex)     //send a signal to the wait chan
		}else if mes.CommandValid && mes.CommandIndex != 1+sc.lastApplied {
			// out of order cmd, just ignore
			sc.logger.L(logger.CtrlerApply, "ignore apply %v for lastApplied %v\n",
			mes.CommandIndex, sc.lastApplied)
		} else {
			// wrong command
			sc.logger.L(logger.CtrlerApply, "Invalid apply msg\n")
		}

		// if sc.raftNeedSnapshot() {
		// 	sc.doSnapshotForRaft(mes.CommandIndex)
		// }

		sc.mu.Unlock()

	}
}

func (sc *ShardCtrler) applyCommand(command Command){

	//make sure command is less than the next seq
	if (command.Seq < sc.next_seq[command.ClientId]){
		return
	}
	if command.Seq != sc.next_seq[command.ClientId]{
		panic("seq gap present!");
	}

	sc.logger.L(raftlogs.CtrlerApply, "This is the recieved command %#v", command)

	//correct command recieved
	sc.next_seq[command.ClientId]++
	if (command.OptType != TYPE_QUERY){
		var new_config *Config
		switch command.OptType{
		case TYPE_JOIN:
			new_config = sc.doJoin(command.Opt.(JoinArgs).Servers)
		case TYPE_LEAVE:
			new_config = sc.doLeave(command.Opt.([]int))
		case TYPE_MOVE:
			new_config = sc.doMove(command.Opt.(GIDandShard))
		}
		sc.configs = append(sc.configs, *new_config)
		sc.logger.L(logger.CtrlerApply, "ctrler change to config %v\n", new_config)
	}
}

//apply snapshot
func (sc *ShardCtrler) applyInstallSnapshot(snap []byte){
	if snap == nil || len(snap) < 1 { 
		sc.logger.L(logger.CtrlerSnap, "empty snap\n")
		return
	}
	
	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)
	lastApplied := 0
	next_seq := make(map[int64]int)
	configs := make([]Config, 1)
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&next_seq) != nil ||
		d.Decode(&configs) != nil {
		sc.logger.L(logger.CtrlerSnap, "apply install decode err\n")
		panic("err decode snap")
	} else {
		sc.lastApplied = lastApplied
		sc.next_seq = next_seq
		sc.configs = configs
	}
}

//hold lock
func (sc *ShardCtrler) doSnapshotForRaft(index int) {
	sc.logger.L(logger.CtrlerSnap, "do snapshot for raft %v %v\n", index, sc.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	lastIndex := sc.lastApplied
	e.Encode(lastIndex)
	e.Encode(sc.next_seq)
	e.Encode(sc.configs)
	sc.rf.Snapshot(index, w.Bytes())
}

//hold lock
func (sc *ShardCtrler) raftNeedSnapshot() bool {
	if sc.maxraftstate == -1 {
		return false
	}
	size := sc.persister.RaftStateSize()
	if size >= sc.maxraftstate {
		sc.logger.L(logger.CtrlerSnapSize, "used size: %d / %d \n", size, sc.maxraftstate)
		return true
	}
	return false
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
	labgob.Register(QueryArgs{})
	
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
	snap := sc.persister.ReadSnapshot()
	sc.applyInstallSnapshot(snap)

	go sc.applier()

	return sc
}
