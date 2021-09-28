package shardctrler

import "fmt"

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	fmt.Printf("join %v\n", args)
	command := Command{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		OptType:  TYPE_JOIN,
		Opt:      *args,
	}
	res := sc.doRequest(&command) 
	reply.WrongLeader = res.(*JoinReply).WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	fmt.Printf("move %v\n", args)
	command := Command{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		OptType:  TYPE_MOVE,
		Opt: GIDandShard{
			GID:   args.GID,
			Shard: args.Shard,
		},
	}
	res := sc.doRequest(&command) 
	reply.WrongLeader = res.(*MoveReply).WrongLeader

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	fmt.Printf("leave %v\n", args)
	command := Command{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		OptType:  TYPE_LEAVE,
		Opt:      args.GIDs,
	}
	res := sc.doRequest(&command) 
	reply.WrongLeader = res.(*LeaveReply).WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	command := Command{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		OptType:  TYPE_QUERY,
		Opt:      args.Num,
	}
	res := sc.doRequest(&command) 
	ress := res.(*QueryReply)
	reply.WrongLeader = ress.WrongLeader
	reply.Config = ress.Config
	//fmt.Printf("server sent reply %#v\n", reply)
}
