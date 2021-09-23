package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
)

const 
(	Debug = false
	TYPE_GET    = 0
	TYPE_PUT    = 1
	TYPE_APPEND = 2
	TYPE_OTHER  = 3
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	OptType  int
	Opt      interface{} //not reference
	Seq int
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()


	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister
	next_seq    map[int64]int   //map of id of clerk and his next_seq
	kv_map map[string]string
	lastApplied int
	logger logger.TopicLogger
	reply_chan map[int]chan bool  //map of each peer's channel
}

func (kv *KVServer) getReplyStruct(req *Command, err Err) interface{} {
	switch req.OptType{
	case TYPE_APPEND:
		fallthrough  //execute next case anyway
	case TYPE_PUT:
		return &PutAppendReply{
			Err: err,
		}
	case TYPE_GET:
		reply := &GetReply{
			Err: err,
		}
		if err == OK { //no error
			reply.Value = kv.kv_map[req.Opt.(string)]
		}
		return reply
	}
	return nil
}

func (kv *KVServer) hasResult(clientId int64, seq int) bool {
	return kv.next_seq[clientId] > seq
}

func (kv *KVServer) doRequest(command *Command) interface{} {
	kv.mu.Lock()
	kv.logger.L(logger.ServerReq, "do request args:%#v \n", command)

	//check if already applied
	if kv.hasResult(command.ClientID, command.Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] already succesfully executed the request\n",
			command.ClientID%1000, command.Seq)
		kv.mu.Unlock()
		return kv.getReplyStruct(command, OK)
	}

	index, _, isLeader := kv.rf.Start(*command)

	if !isLeader {
		kv.logger.L(logger.ServerReq, "declined [%3d--%d] for not leader\n",
			command.ClientID%1000, command.Seq)
		kv.mu.Unlock()
		return kv.getReplyStruct(command, ErrWrongLeader)
	}else {
		kv.logger.L(logger.ServerStart, "start [%3d--%d] as leader?\n",
			command.ClientID%1000, command.Seq)
	}

	wait_chan := kv.getWaitChan(index)
	kv.mu.Unlock()

	timeout := time.NewTimer(time.Millisecond * 200)	//wait to see if operation will be done

	//select will block until it recieves either
	select {
	case <-wait_chan:
	case <-timeout.C:
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	if kv.hasResult(command.ClientID, command.Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] successed !!!!! \n",
			command.ClientID%1000, command.Seq)
		return kv.getReplyStruct(command, OK)
	} else {

		kv.logger.L(logger.ServerReq, "[%3d--%d] failed applied \n",
			command.ClientID%1000, command.Seq)
		return kv.getReplyStruct(command, ErrNoKey)
	}
}

//hold lock
func (kv *KVServer) getWaitChan(index int) chan bool {
	if _, ok := kv.reply_chan[index]; !ok{
		//create the channel
		kv.reply_chan[index] = make(chan bool)
	}
	return kv.reply_chan[index]
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(KeyValue{})
	
	kv := new(KVServer)
	kv.me = me 
	kv.logger = logger.TopicLogger{
		Me: me,
	}
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 30)
	kv.reply_chan = make(map[int]chan bool)
	kv.lastApplied = 0
	kv.kv_map = make(map[string]string)
	kv.next_seq = make(map[int64]int)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//snap := kv.persister.ReadSnapshot()
	//kv.applyInstallSnapshot(snap)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//go kv.applier()

	// You may need initialization code here.
	kv.logger.L(logger.Log2, "S%d is now alive", kv.me)
	return kv
}
