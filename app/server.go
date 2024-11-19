package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {

	//!Parsing and reading cmd arguments.

	cmdArgs := os.Args[1:];

	var dirName string;
	var rdbFileName string;
	var port int;
	port = 6379;
	var masterAddress string
	for index, arg := range cmdArgs {
		switch arg {
		case "--dir":
			dirName = cmdArgs[index + 1]		
		case "--dbfilename":
			rdbFileName = cmdArgs[index + 1]
		case "--port":
			port, _ = strconv.Atoi(cmdArgs[index + 1])
		case "--replicaof":
			ma := cmdArgs[index + 1]		
			compAdd := 	strings.Split(ma, " ")
			ipAdd := compAdd[0]
			ipPort := compAdd[1]
			masterAdd := ipAdd + ":"+ ipPort
			masterAddress = masterAdd
		default:
			continue;
		}
	}

	portString := ":" + strconv.Itoa(port);

	fmt.Println("Printing all the cmd line arguments: ", cmdArgs)

	server, _ := NewRedisServer(portString, masterAddress, dirName, rdbFileName)

	defer server.listener.Close()
	server.eventLoopStart();

	//!There won't be any switching until leader election happns for consensus, I don't think
	// we will implement that

}
