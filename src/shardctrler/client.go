package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var used_ID map[int64]bool
var map_lock sync.Mutex

func init(){
	used_ID = make(map[int64]bool)
	used_ID[-1] = true
}

func  getUnusedClientId() int64{
	map_lock.Lock()
	defer map_lock.Unlock()
	for{
		num := nrand()
		if !used_ID[num]{
			used_ID[num] = true
			return num
		}
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id int64
	seq int
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
	// Your code here.
	ck.id = getUnusedClientId()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
		Seq: ck.seq,
		ClientId: ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++ //increment the seq because server accepted
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		Seq: ck.seq,
		ClientId: ck.id,
	}


	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		Seq: ck.seq,
		ClientId: ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID: gid,
		Seq: ck.seq,
		ClientId: ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
