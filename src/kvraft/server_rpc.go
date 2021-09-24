package kvraft

import logger "6.824/raft-logs"

//server is replying to client code
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	req_args := &Command{
		ClientID: args.ClientId,
		OptType: TYPE_GET,
		Opt: args.Key,
		Seq: args.Seq,
	}

	res := kv.doRequest(req_args).(*GetReply)

	reply.Err = res.Err
	reply.Value = res.Value

	kv.logger.L(logger.ServerReq, "[%3d--%d] get return result%#v\n",
		args.ClientId%1000, args.Seq, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	req_args := &Command{
		ClientID: args.ClientId,
		Opt: KeyValue{
			Key: args.Key,
			Value: args.Value,
		},
		Seq: args.Seq,
	}

	//decide whether this is a put or an append
	switch args.Op{
	case "Put":
		req_args.OptType = TYPE_PUT
	case "Append":
		req_args.OptType = TYPE_APPEND
	default:
		kv.logger.L(logger.ServerReq, "putAppend err type %d from [%3d--%d]\n",
			args.Op, args.ClientId%1000, args.Seq)	
	}

	res := kv.doRequest(req_args).(*PutAppendReply)

	reply.Err = res.Err

	kv.logger.L(logger.ServerReq, "[%3d--%d] putAppend return result%#v\n",
		args.ClientId%1000, args.Seq, reply)
	
}
