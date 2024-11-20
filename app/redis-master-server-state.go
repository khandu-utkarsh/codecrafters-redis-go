package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// MasterState is the state for the Redis server when it acts as a master
type MasterState struct{
	replcas 	map[int]*net.TCPConn	//!Fd to tcpConnection
	forwardingReqBuffer map[int][]byte	//!Fd to data to send

	//!Req, response	
}

// HandleRequest processes the request for the master server
func (m *MasterState) HandleRequest(reqData [][]byte, server *RedisServer) ([]byte, error) {
	fmt.Println("Master state handling the request.")


	//!Handle request on the basis of if it is a replica or master.
	var response []byte

	cmdName := strings.ToLower(string(reqData[0]));
	switch cmdName {		
	//	------------------------------------------------------------------------------------------  //
	case "psync":	//!Called on master
		if string(reqData[1]) == "?" && string(reqData[2]) == "-1" {
			out := "+FULLRESYNC " + server.master_replid + " " + strconv.Itoa(server.master_repl_offset) + "\r\n"
			response = append(response, []byte(out)...)
			// Hex string of the content of RBD File -- Got it from the github
			hexString := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
			// Convert hex string to []byte
			rdbContentBytes, err := hex.DecodeString(hexString)
			if err != nil {
				log.Fatalf("Error decoding rdb file content hex string: %v", rdbContentBytes)
			}
			prefixString := "$" + strconv.Itoa(len(rdbContentBytes)) + "\r\n"
			fmt.Println("Prefix string: ", prefixString)
			response = append(response, []byte(prefixString)...)
			response = append(response, rdbContentBytes...)
		}

	//	------------------------------------------------------------------------------------------  //
	case "set": //!Differnt behavior for master and replica
		if(len(reqData) < 3) {
			fmt.Println("Minimum args req are 3")
		} else {
			key := string(reqData[1]);
			value := string(reqData[2]);

			//!Check if information of time present
			var vt ValueTickPair;
			vt.value = value;
			if len(reqData) > 3 {	//!Time included
				timeUnit := strings.ToLower(string(reqData[3]))
				timeDuration, _ := strconv.Atoi(string(reqData[4]))
				if(timeUnit == "px") {
					timeDuration = timeDuration * 1000000;	//!Mili to nano seconds
				} else {
					fmt.Println("Time unit not known");					
				}
				vt.tickUnixNanoSec = time.Now().UnixNano() + int64(timeDuration); 
			} else {
				vt.tickUnixNanoSec = -1;	//!Never expires
			}
			server.database[key] = vt;
			out := "+" + "OK" + "\r\n"			
			response = []byte(out)
			fmt.Println("Here in set: ", server.database)
		}


	//	------------------------------------------------------------------------------------------  //
	case "info":
		if(len(reqData) < 2) {
			fmt.Println("Nothing requested with replication. Ideally should return all the information about the server, but currently returning none.")
		} else {
			if(strings.ToLower(string(reqData[1])) == "replication") {
				rolev := "master"
				retstr := "role:" + rolev
				retstr +="\n"
				retstr += "master_replid:" + server.master_replid
				retstr +="\n"
				retstr += "master_repl_offset:" + strconv.Itoa(server.master_repl_offset);
				out := createBulkString(retstr);
				response = []byte(out)
			}	
		}	

	//	------------------------------------------------------------------------------------------  //		
	default:
		fmt.Println("Not implementation found in master state for, ", cmdName);
	}
	return response, nil;	
}

// ForwardRequest forwards request data from the master to replicas
func (m *MasterState) ForwardRequest(reqData []byte, server *RedisServer) {
	if len(m.replcas) == 0 {
		return
	}
	fmt.Println("Master forwarding request to replica")
	for fd := range m.replcas {
		// Add request data to buffer for forwarding
		server.forwardingReqBuffer[fd] = append(server.forwardingReqBuffer[fd], reqData...)
	}
}
