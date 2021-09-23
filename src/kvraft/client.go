package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	logger "6.824/raft-logs"
)

var used_ID map[int64]bool
var map_lock sync.Mutex

func init() {
	used_ID = make(map[int64]bool)
	used_ID[-1] = true
}

func getUnusedClientID() int64 {
	map_lock.Lock()
	defer map_lock.Unlock()
	for {
		id := nrand()
		if !used_ID[id] {
			used_ID[id] = true
			return id
		}
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	logger logger.TopicLogger
	id int64
	seq int   //to keep track of each req seq number, so as not to execute the same req twice
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = getUnusedClientID()
	ck.seq = 0
	ck.logger = logger.TopicLogger{Me: int(ck.id) % 1000}
	ck.logger.L(logger.Clerk, "K%d is now alive", ck.id)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := &GetArgs{
		Key: key,
		ClientId: ck.id,
		Seq: ck.seq,
	}

	for {
		for i := range ck.servers {		

			reply := &GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", args, reply)
			ck.logger.L(logger.Clerk, "K%d sent a request to K%d", ck.id, i)
			if ok && reply.Err == OK{
				ck.logger.L(logger.Clerk, 
					"K%d has sent a get req and was accepted by K%d", ck.id, i)
				ck.seq++
				return reply.Value
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.id,
		Seq: ck.seq,
	}

	//keep sending until a server replies that it has accepted the request
	for {
		for i := range ck.servers {			
			reply := &PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
			ck.logger.L(logger.Clerk, "K%d sent a request to K%d", ck.id, i)
			if ok && reply.Err == OK{
				ck.logger.L(logger.Clerk, 
					"K%d has sent a putAppend req and was accepted by K%d", ck.id, i)
				ck.seq++
				return
			}
		}
		time.Sleep(time.Millisecond * 100)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.logger.L(logger.Clerk, "K%d was called with a put of key %v value %v", ck.id, key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.logger.L(logger.Clerk, "K%d was called with an append of key %v value %v", ck.id, key, value)
	ck.PutAppend(key, value, "Append")
}
