# 6.824_MIT_Distributed_Systems
# MIT 6.824 Spring 2021 Labs

6.824 is a core graduate subject with lectures, labs, an optional project, a mid-term exam, and a final exam. 6.824 is 12 units.

## Labs:

Course website: https://pdos.csail.mit.edu/6.824/

- [x] Lab 1: MapReduce

- [x] Lab 2: Raft Consensus Algorithm
  - [x] Lab 2A: Raft Leader Election
  - [x] Lab 2B: Raft Log Entries Append
  - [x] Lab 2C: Raft state persistence
  - [x] Lab 2D: Raft log compaction
  
- [x] Lab 3: Fault-tolerant Key/Value Service
  - [x] Lab 3A: Key/value service without snapshots
  - [x] Lab 3B: Key/value service with snapshots

- [ ] Lab 4: Sharded Key/Value Service
  - [x] Lab 4A: The Shard controller
  - [ ] Lab 4B: Sharded Key/Value Server

## Deploy
Install golang, and setup golang environment variables and directories. [Click here](https://golang.org/doc/install) to learn it.

```
cd $GOPATH
git clone https://github.com/Mohamed247/6.824_MIT_Distributed_Systems.git
cd 6.824
export GOPATH=$GOPATH:$(pwd)
```

## Test and run

### Lab 1

To run the coordinator
```
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
```

To run the workers, in one or more windows
```
go run -race mrworker.go wc.so
```

### Lab 2

```
cd src/raft
```

Without Logs
```
VERBOSE=0 go test -run 2 -race | python3 ../raft-only-logs/dslogs.py -c 5
```

With Logs
```
VERBOSE=1 go test -run 2 -race | python3 ../raft-only-logs/dslogs.py -c 5
```

To run 100 rounds of lab 2 with 8 in parallel
```
python3 passOrFailChecker.py -n 100 -p 8 2
```

### Lab 3

```
cd src/kvraft
```

Without Raft Logs
```
VERBOSE=0 go test -run 3 -race 
```

With Raft Logs
```
VERBOSE=1 go test -run 3 -race | python3 ../raft-logs/color_print.py -c 5
```

To run 100 rounds of lab 3 with 8 in parallel
```
python3 passOrFailChecker.py -n 100 -p 8 3
```

### Lab 4

```
cd src/shardctrler
```

Without Raft Logs
```
VERBOSE=0 go test -race
```

With Raft Logs
```
VERBOSE=1 go test -race | python3 ../raft-logs/color_print.py -c 5
```
