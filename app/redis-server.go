package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ServerState defines common methods for both Master and Replica states
type ServerState interface {
	HandleRequest(reqData [][]byte, server *RedisServer, clientConn *net.TCPConn) ([]byte, error)
	ForwardRequest(reqData []byte, server *RedisServer)
}


// RedisServer context holds state and relevant server details
type RedisServer struct {
	listener    *net.TCPListener
	port        int
	
	master_replid string
	master_repl_offset int
	
	state       ServerState

	pollFds     map[int]unix.PollFd	//!Fd to unix polling objs
	clients     map[int]*net.TCPConn
	database    map[string]ValueTickPair
	//!Req, response
	requestResponseBuffer map[int][]byte	//!Fd tp response
	forwardingReqBuffer map[int][]byte //!Fd to forwarding req

	rdbDirPath string
	rdbFileName string
}

// RequestHandler processes the input and returns the response based on server state
func (server *RedisServer) RequestHandler(reqData [][]byte, clientConn *net.TCPConn) ([]byte, error) {

	//!Let's parse the request here, and send it appropriately to  master or slave handle
	//!Read request everyone can respond to
	//fmt.Println("Inside redis server request handler")

	//!Handle request on the basis of if it is a replica or master.
	var response []byte
	var err error

	cmdName := strings.ToLower(string(reqData[0]));
	switch cmdName {

	//	------------------------------------------------------------------------------------------  //
	case "ping":
		out := "+" + "PONG" + "\r\n"
		response = []byte(out)

	//	------------------------------------------------------------------------------------------  //
	case "echo":
		//!Get byte array of all the rest elements
		var obytes []byte
		for elemIndex, elem := range reqData {
			if(elemIndex == 0) {
				continue;
			}
			obytes = append(obytes, elem...)
		}
		out := createBulkString(string(obytes));
		response = []byte(out)

	//	------------------------------------------------------------------------------------------  //
	case "get":
		if(len(reqData) < 2) {
			fmt.Println("Minimum args req are 2")
		} else {
			var out string
			vt, ok := server.database[string(reqData[1])]		
			if ok {
				currTime := time.Now().UnixNano();				
				if vt.tickUnixNanoSec  == -1 || vt.tickUnixNanoSec > currTime {
					out = createBulkString(vt.value);
				} else {
					fmt.Println("Current time: ", currTime);
					fmt.Println("vt.timeout: ", vt.tickUnixNanoSec)
					out = "$-1\r\n"					
				}
			} else {
				out = "$-1\r\n"
			}
			response = []byte(out)
		}


	//	------------------------------------------------------------------------------------------  //
	case "config":
		if(len(reqData) < 3) {
			fmt.Println("Minimum args req are 3 for config")
		} else {
			var out string
			if strings.ToLower(string(reqData[1])) == "get" && string(reqData[2]) == "dir" {
				oa := make([]string, 2)
				oa[0] = createBulkString("dir");
				oa[1] = createBulkString(server.rdbDirPath);
				out += createRESPArray(oa);
			} else if strings.ToLower(string(reqData[1])) == "get" && string(reqData[2]) == "dbfilename" { 
				oa := make([]string, 2)
				oa[0] = createBulkString("dbfilename");
				oa[1] = createBulkString(server.rdbFileName);
				out += createRESPArray(oa);
			} else {
				fmt.Println("Nothing implemented for this value of config");				
			}
			response = []byte(out)
		}



	//	------------------------------------------------------------------------------------------  //
	case "keys":
		if(len(reqData) < 2) {
			fmt.Println("Minimum args req are 3 for config")
		} else {
			for qkindex, qkey := range reqData {
				
				if qkindex < 1 {
					continue;
				}
				
				if(string(qkey) == "*") {
					var keysout []string
					for k := range server.database {
						kstr := createBulkString(k);
						keysout = append(keysout, kstr)
					}
					out := createRESPArray(keysout);
					response = []byte(out)
				}				
			}
		}
		
	//	------------------------------------------------------------------------------------------  //		
	default:
		response, err = server.state.HandleRequest(reqData, server, clientConn)
	}
	return response, err

}

//!Sort of constructor
// RedisServer struct maintains the current behavior and can switch between master and replica
func NewRedisServer(port int, masterAddress string, rdbDicPath string, rdbFilePath string) (*RedisServer, error) {
	address := ":" + strconv.Itoa(port);
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("error listening on port %s: %v", address, err)
	}

	server := &RedisServer{
		port: port,
		listener:     listener.(*net.TCPListener),
		clients:      make(map[int]*net.TCPConn),
		pollFds:      make(map[int]unix.PollFd),
		database:     make(map[string]ValueTickPair),
		requestResponseBuffer: make(map[int][]byte),
		forwardingReqBuffer: make(map[int][]byte),		
		rdbDirPath:   rdbDicPath,
		rdbFileName:  rdbFilePath,
		master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		master_repl_offset: 0,
	}

	//!As soon as we create add it to the polling. -- Helps
	lfd, _ := GetTCPListenerFd(server.listener)
	server.pollFds[lfd] = unix.PollFd{Fd: int32(lfd), Events: unix.POLLIN}

	//!Load the data from rdb
	rdbManager := &RDBFileManager{
		directoryPath : server.rdbDirPath,
		dBName : server.rdbFileName,
	}
	rdbManager.LoadDatabase(server);	//!Loading databse into memory


	if masterAddress == "" {
		server.state =&MasterState{
			replcas: make(map[int]*net.TCPConn),
		}
	} else {

		//!Establish the connection to master here, doing the three step handshake, and if we get any data on socket, process it

		server.state = &ReplicaState{
			masterAddress: masterAddress,
		}

		repState, _ := server.state.(*ReplicaState)
		//!Do the handshake to connect the replica to master
		repState.doReplicationHandshake(server)
	}

	// Default to MasterState when creating the server
	return server, nil
}

// SwitchToMaster switches the server to behave as a master
func (server *RedisServer) SwitchToMaster() {
	server.state = &MasterState{}
}

// SwitchToReplica switches the server to behave as a replica
func (server *RedisServer) SwitchToReplica() {
	server.state = &ReplicaState{}
}

func (server *RedisServer) eventLoopStart() {
	fmt.Println("Inside event loop")
	//!Getting done on creation
	// // Add listener FD to pollFds for read events
	// listenerFd, _ := GetTCPListenerFd(server.listener)
	// lfd := int(listenerFd)
	//server.pollFds[lfd] = unix.PollFd{Fd: int32(lfd), Events: unix.POLLIN};
	fmt.Println("PollFds on event loop start: ", server.pollFds)

	// ms, msok := server.state.(*MasterState)
	// rs, rsok := server.state.(*ReplicaState)

	// Main event loop
	for {	
		//fmt.Println("Iteration")

		//!Creating a poll fd slice
		var pollFdsSlice []unix.PollFd;
		for _, pfd := range server.pollFds {
			pollFdsSlice = append(pollFdsSlice, pfd);
		}
		// Poll for events
		n, err := unix.Poll(pollFdsSlice, -1) // Wait indefinitely for I/O events
		if err != nil {
			fmt.Println("Error polling:", err)
			break
		}

		lfd, _ := GetTCPListenerFd(server.listener)
		// Handle each event
		for i := 0; i < n; i++ {
			fd := int(pollFdsSlice[i].Fd)
			if fd == lfd && pollFdsSlice[i].Revents & unix.POLLIN != 0 {
				//!TCP Server fd --> Must be new connection req

				// New connection request
				conn, err := server.listener.Accept()
				if err != nil {
					fmt.Println("Error accepting connection:", err)
					continue
				}
	
				// Add new connection to pollFds for read/write events
				tcpConn := conn.(*net.TCPConn)
				connFd, _ := GetTCPConnectionFd(tcpConn)

				server.pollFds[connFd] = unix.PollFd{ Fd: int32(connFd), Events: unix.POLLIN | unix.POLLOUT,};
				server.clients[connFd] = tcpConn
				fmt.Println("Accepted new connection:", conn.RemoteAddr())
			} else {

				clientConn, ok := server.clients[fd]
				if !ok {
					continue;
					//fmt.Println("Who added this in polling list")			
				}
	
				// Check for read events (data from user)
				if pollFdsSlice[i].Revents & unix.POLLIN != 0 {
					buffer := make([]byte, 1024)
					n, err := clientConn.Read(buffer)
					if(err != nil) {	//!Assuming work has been done, we can close it
						fmt.Println("Checking for data: ",string(buffer[:n]))
						fmt.Println("Closing fd:", fd, "|error:", err.Error())
						clientConn.Close()
						delete(server.clients, fd)
						delete(server.pollFds, fd)
					} else {

						//!What I can do in this is after parsing, if it is set, then only forward it, else not
						inputCommands, inpCmdsSize := server.getCmdsFromInput(buffer[:n])



						//!Process each command individually
						for currCmdIndex, inpCmd := range inputCommands{
							if strings.ToLower(string(inpCmd[0])) == "set" { 						//!Only forward cmds 
								server.state.ForwardRequest(buffer[:n], server)	//!Forwarding the req to all replicas in raw byte forms
							}
							outbytes, _ := server.RequestHandler(inpCmd, clientConn)
							server.master_repl_offset += inpCmdsSize[currCmdIndex]
							
							if len(outbytes) != 0 {
								server.requestResponseBuffer[fd] = append(server.requestResponseBuffer[fd], outbytes...)
							}
						}
					}
				}

				// Check for write events (socket is ready to send data)
				if pollFdsSlice[i].Revents & unix.POLLOUT != 0 {
					if data, ok := server.requestResponseBuffer[fd]; ok && len(data) > 0 {
						clientConn.Write(server.requestResponseBuffer[fd])
						delete(server.requestResponseBuffer, fd)
					}
					if data, ok := server.forwardingReqBuffer[fd]; ok && len(data) > 0 {
						fmt.Println("Req frowards to ", clientConn)
						clientConn.Write(server.forwardingReqBuffer[fd])
						delete(server.forwardingReqBuffer, fd)
					}
				}
			}
		}
	}
	fmt.Println("Closing the event loop")
}