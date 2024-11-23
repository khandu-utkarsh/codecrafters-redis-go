package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ReplicaState is the state for the Redis server when it acts as a replica
type ReplicaState struct{
	masterAddress string	
	masterConn *net.TCPConn
	masterFd int
}

// HandleRequest processes the request for the replica server
func (r *ReplicaState) HandleRequest(reqData [][]byte, reqSize int, server *RedisServer, clientConn *net.TCPConn) ([]byte, error) {
	//fmt.Println("Replicate handling request")


	//!Handle request on the basis of if it is a replica or master.
	var response []byte

	cmdName := strings.ToLower(string(reqData[0]));
	switch cmdName {

	//	------------------------------------------------------------------------------------------  //
	case "replconf": //!Called on replica
		var out string
		if(string(reqData[1]) == "GETACK" && string(reqData[2]) == "*") {
			oa := make([]string, 3)
			oa[0] = createBulkString("REPLCONF");
			oa[1] = createBulkString("ACK");
			oa[2] = createBulkString(strconv.Itoa(server.master_repl_offset));
			out += createRESPArray(oa);
			//fmt.Println("Sending out message: ", out);
		}
		response = []byte(out)

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
			//out += "+" + "OK" + "\r\n"	//!No response on replicate
			fmt.Println("Here in set: ", server.database)
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
			//var out string
			sv, ok := server.database_stream[skey]

			lastEntryId := "0-0"
			if ok {
				lastEntryId = sv.entries[len(sv.entries) - 1].id
			}
			_, entryId = generateId(entryId, lastEntryId)
			validated, _ := validateString(entryId, lastEntryId)
			if(validated) {
				sentry.id = entryId
				sv.entries = append(sv.entries, sentry)
				server.database_stream[skey] = sv
				//out = createBulkString(entryId);					
			} else {
				//out = errString
			}
			//out := createBulkString(entryId);
		}	



	//	------------------------------------------------------------------------------------------  //
	case "info":
		if(len(reqData) < 2) {
			fmt.Println("Nothing requested with replication. Ideally should return all the information about the server, but currently returning none.")
		} else {
			if(strings.ToLower(string(reqData[1])) == "replication") {
				rolev := "slave"
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
		fmt.Println("Not implementation found in replicate state for cmd: ",cmdName);
	}
	return response, nil
}

// ForwardRequest does nothing for replicas, as they don’t forward requests
func (r *ReplicaState) ForwardRequest(reqData []byte, server *RedisServer) {
	// Replicas do not forward requests
}

func (r * ReplicaState) doReplicationHandshake(server *RedisServer) (*net.TCPConn) {
	//!Dial to create connection
	conn, err := net.Dial("tcp", r.masterAddress)
	if err != nil { 
		fmt.Printf("Error connecting to master server: %v\n", err)
		return nil
	}

	// //!We should add this to polling here
	// r.masterConn = conn.(*net.TCPConn)
	// r.masterFd, _ = GetTCPConnectionFd(r.masterConn)

	mcnn := conn.(*net.TCPConn)
	mfd, _ := GetTCPConnectionFd(mcnn)

	fmt.Printf("Connected to server at %s\n", r.masterAddress)

	//!Performing handshake protocol:

	messages := []string{"PING", "REPLCONF", "REPLCONF", "PSYNC"}
	for idx, message := range messages {

		//!This is creating request message for handshake
		var out string
		if idx == 0 {
			oa := make([]string, 1)
			oa[0] = message
			oa[0] = createBulkString(oa[0]);
			out = createRESPArray(oa);
		}	else if idx == 1 {
			oa := make([]string, 3)
			oa[0] = message
			oa[1] = "listening-port"
			oa[2] = strconv.Itoa(server.port)			
			oa[0] = createBulkString(oa[0]);
			oa[1] = createBulkString(oa[1]);
			oa[2] = createBulkString(oa[2]);
			out = createRESPArray(oa);
			// fmt.Println("Index 1: ", out)
		} else if(idx == 2) {
			oa := make([]string, 3)
			oa[0] = message
			oa[1] = "capa"
			oa[2] = "psync2"

			oa[0] = createBulkString(oa[0]);
			oa[1] = createBulkString(oa[1]);
			oa[2] = createBulkString(oa[2]);
			out = createRESPArray(oa);
		} else if(idx == 3) {
			//"Since it is the first time connecting and we don't know any info about the master, hence sending this:"
			oa := make([]string, 3)
			oa[0] = message
			oa[1] = "?"
			oa[2] = "-1"

			oa[0] = createBulkString(oa[0]);
			oa[1] = createBulkString(oa[1]);
			oa[2] = createBulkString(oa[2]);
			out = createRESPArray(oa);				
		} else {
			fmt.Println("Some random index in handshake")
		}

		//!Sending out handshake messages
		//fmt.Println("Sending out on handshake: ", out);
		_, err = conn.Write([]byte(out))
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			return nil
		}

		//!Waiting for handshake repsonse
		//!After writing, we will be reading
		//!Highly possible that as soon as we connect, server sends us all the pending messages, so process them and store them as well.

		if(idx == 3) {
			//!As soon as we sent last pysnc, add this  to polling so that we don't miss out anything after reading
			server.pollFds[mfd] = unix.PollFd{ Fd: int32(mfd), Events: unix.POLLIN | unix.POLLOUT,};	//!Only polling in, won't be writing to master
			server.clients[mfd] = mcnn	//!Map of fd to tcpConns
		}

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Close it replication error. ", err)
			conn.Close()
			r.masterConn = nil
			delete(server.clients, mfd)
			delete(server.pollFds, mfd)
		} else {
			//!Reading success

			inputCommands, inpCmdsSize := server.getCmdsFromInput(buffer[:n])			
			for currCmdIndex, inpCmd := range inputCommands{				//!Process each command individually
				outbytes, _ := server.RequestHandler(inpCmd,inpCmdsSize[currCmdIndex], conn.(*net.TCPConn))
				if(idx == 3) {
					//!Handhsake success, write idx 3 and then read idx 3
					if(currCmdIndex == 1) {	//!This will be the file sent by master, once this is received mark this as replica
						r.masterConn = mcnn	//!Assigning the master field
						r.masterFd = mfd	//!Assigning the master field
						fmt.Println("Current server added as replica for the master at address: ", r.masterAddress)	
					} else if(currCmdIndex > 1) {	//!Some prev cmd arrived, add these to offset values:
						server.master_repl_offset += inpCmdsSize[currCmdIndex]
					}
				}
				//!Adding processed request
				if len(outbytes) != 0 {
					server.requestResponseBuffer[mfd] = append(server.requestResponseBuffer[mfd], outbytes...)
				}
			}
		}
	}
	return r.masterConn
}
