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
func (r *ReplicaState) HandleRequest(reqData [][]byte, server *RedisServer, clientConn *net.TCPConn) ([]byte, error) {
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
			fmt.Println("Sending out: ", out);
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

	//!We should add this to polling here
	r.masterConn = conn.(*net.TCPConn)
	r.masterFd, _ = GetTCPConnectionFd(r.masterConn)

	//!Add to creation after polling, so that it is getting tracked from the start
	server.pollFds[r.masterFd] = unix.PollFd{ Fd: int32(r.masterFd), Events: unix.POLLIN | unix.POLLOUT,};	//!Only polling in, won't be writing to master
	server.clients[r.masterFd] = r.masterConn	//!Map of fd to tcpConns

	fmt.Printf("Connected to server at %s\n", r.masterAddress)

	//!Performing handshake protocol:

	messages := make([]string, 4)
	messages[0] = "PING"
	messages[1] = "REPLCONF"
	messages[2] = "REPLCONF"
	messages[3] = "PSYNC"

	for idx, message := range messages {
		var out string
		if idx == 1 {
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
			oa := make([]string, 1)
			oa[0] = message
			oa[0] = createBulkString(oa[0]);
			out = createRESPArray(oa);
		}
		_, err = conn.Write([]byte(out))
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			return nil
		}

		//!After writing, we will be reading
		//!Highly possible that as soon as we connect, server sends us all the pending messages, so process them and store them as well.

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Close it replication error. ", err)
			conn.Close()
			r.masterConn = nil
			delete(server.clients, r.masterFd)
			delete(server.pollFds, r.masterFd)
		} else {
			//fmt.Println("Handshake request: ", message, " |Handshake response: ", string(buffer));
			inputCommands, inpCmdsSize := server.getCmdsFromInput(buffer[:n])			
			//!Process each command individually
			for currCmdIndex, inpCmd := range inputCommands{
				//fmt.Println("Cmds are: ", inputCommands)
				outbytes, _ := server.RequestHandler(inpCmd,conn.(*net.TCPConn))
				//fmt.Println("Curr cmd size 1: ", inpCmdsSize[currCmdIndex])

				if(idx == 3 && currCmdIndex > 1) {
					
					server.master_repl_offset += inpCmdsSize[currCmdIndex]
					//fmt.Println("Server size: ", server.master_repl_offset)
				}
				if len(outbytes) != 0 {
					server.requestResponseBuffer[r.masterFd] = append(server.requestResponseBuffer[r.masterFd], outbytes...)
				}
			}
		}
	}

	return r.masterConn
}
