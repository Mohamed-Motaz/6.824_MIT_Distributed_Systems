package raftlogs

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
	debug = 1
)

var debugStart time.Time
var debugVerbosity int = -1;

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
func initDebugger() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

type Logger struct{

}

func (lg *Logger ) Log(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity == -1 {
		 initDebugger();
	}
	
	if debug >= 1 && debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}