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
	replicasOffset map[int]int	//!Replica offset information
	replicaAckAnswered int
	replicaAckAsked int
	//!Req, response	
}

// HandleRequest processes the request for the master server
func (m *MasterState) HandleRequest(reqData [][]byte, reqSize int, server *RedisServer, clientConn *net.TCPConn) ([]byte, error) {
	fmt.Println("Master state handling the request.")


	//!Handle request on the basis of if it is a replica or master.
	var response []byte

	cmdName := strings.ToLower(string(reqData[0]));
	switch cmdName {		

	//	------------------------------------------------------------------------------------------  //
	case "replconf":
		if len(reqData) == 3 && (string(reqData[1]) == "listening-port") || (string(reqData[1]) == "capa" && string(reqData[2]) == "psync2") {
			out := "+" + "OK" + "\r\n"
			response = []byte(out)
		} else if len(reqData) == 3 && string(reqData[1]) == "ACK" {

			fmt.Println("Testing...", m.replicaAckAnswered, " and we asked for: ", m.replicaAckAsked)
			conn_repl_offset, _ := strconv.Atoi(string(reqData[2]))
			connFd, _ := GetTCPConnectionFd(clientConn)
			m.replicasOffset[connFd] = conn_repl_offset
			m.replicaAckAnswered++;
			if(m.replicaAckAnswered >= m.replicaAckAsked) {
				fmt.Println("Enough acks rec...", m.replicaAckAnswered, " and we asked for: ", m.replicaAckAsked)
				fmt.Println("Num of timers before deletion...", len(server.timers))
				server.timers[0].callback()	//!This should execute the timer for sure.
				server.timers = make([]Timer, 0)
			}
		} else {
			fmt.Println("Don't know what to do here in replconf");
		}

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
			//fmt.Println("Prefix string: ", prefixString)
			response = append(response, []byte(prefixString)...)
			response = append(response, rdbContentBytes...)
			//fmt.Println("Dbg response: ", string(response))

			//!Add this stage handshake has been successful, add replicas
			cfd, _ := GetTCPConnectionFd(clientConn)
			m.replcas[cfd] = clientConn
			m.replicasOffset[cfd] = 0	//!Since we recv -1  in req, means it is just starting
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
			vt.keyType = "string"
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
			server.cmdProcessed ++;
		}

	//	------------------------------------------------------------------------------------------  //
	case "incr": //!Differnt behavior for master and replica
		if(len(reqData) < 2) {
			fmt.Println("Minimum args req are 2")
		} else {
			
			var out string
			key := string(reqData[1]);
			
			vt, ok := server.database[key]			
			if !ok {
				vt = ValueTickPair{value: "1", tickUnixNanoSec: -1}
			} else {
				iv, err := strconv.Atoi(vt.value)
				if err != nil {
					out = "-ERR value is not an integer or out of range\r\n"
				} else {
					iv++;
					vt.value = strconv.Itoa(iv)
				}
			}
			server.database[key] = vt;
			fmt.Println("Here in incr: ", server.database)
			if out == ""{
				out = ":" + vt.value + "\r\n"	
			}
			response = []byte(out)
			server.cmdProcessed ++;
		}



	//	------------------------------------------------------------------------------------------  //
	case "xadd":
		if(len(reqData) < 2) {
			fmt.Println("Min agrs needed by ", string(reqData[0]), " are more then 2.")
		} else {
			skey := string(reqData[1]);			
			entryId:= string(reqData[2])

			ess := make([]FundamentalStreamEntry, 0)
			for i:= 3; i < len(reqData);i = i + 2 {
				currk:= string(reqData[i])
				cv := string(reqData[i + 1])
				fe := FundamentalStreamEntry{key: currk, value: cv}
				ess = append(ess, fe)
			}
			sentry := StreamEntry{}
			sentry.id = entryId
			sentry.kvpairs = ess
			
			//!Before pushing it, validate it			
			var out string
			sv, ok := server.database_stream[skey]

			lastEntryId := "0-0"
			if ok {
				lastEntryId = sv.entries[len(sv.entries) - 1].id
			}
			_, entryId = generateId(entryId, lastEntryId)
			validated, errString := validateString(entryId, lastEntryId)
			if(validated) {
				sentry.id = entryId
				sv.entries = append(sv.entries, sentry)
				server.database_stream[skey] = sv
				out = createBulkString(entryId);					
			} else {
				out = errString
			}
			fmt.Println("Printing stream database: ", server.database_stream)
			response = []byte(out)

			//!New entry pushed, check if any pending xread command, if yes this is the time to execute
			for k, sc := range server.database_stream_xread_fxns {
				if k == skey {
					fmt.Println("Executing callback on xread: ")
					sc.Callback()
					for _, dk := range sc.Streams {
						delete( server.database_stream_xread_fxns, dk)
					}

				} 
			}
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
	case "wait":
		if(len(reqData) < 3) {
			fmt.Println("Not all cmds passed with WAIT.")
		} else {
			rep_count_asked, _ := strconv.Atoi(string(reqData[1]))
			timeout_provided, _ := strconv.Atoi(string(reqData[2]))


			if len(m.replcas) == 0 || server.cmdProcessed == 0 {
				//!Send instant response
				fmt.Println("Entering this zero replica thing: ")
				out := createIntegerString(len(m.replcas))
				response = []byte(out)
			} else {
				m.replicaAckAnswered = 0
				//!Message all replicas that request offset
				for rfd :=range m.replcas {
					server.requestResponseBuffer[rfd] = []byte(createGetAckString())
				}
				m.replicaAckAsked = rep_count_asked
				fmt.Println("Added the timer")
				server.AddTimer(time.Duration(timeout_provided)*time.Millisecond, func() {
					output := createIntegerString(m.replicaAckAnswered)
					response = []byte(output)
					clientConn.Write(response)	//!Writing the reponse on callback, once this timer is executed
					fmt.Println("Writing inside callback: ", string(response))
				})
				fmt.Println("Timers after addition: ", len(server.timers), " | ", server.timers)
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
