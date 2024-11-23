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
	HandleRequest(reqData [][]byte, reqSize int, server *RedisServer, clientConn *net.TCPConn) ([]byte, error)
	ForwardRequest(reqData []byte, server *RedisServer)
}


// RedisServer context holds state and relevant server details
type RedisServer struct {
	listener    *net.TCPListener
	port        int
	
	master_replid string
	master_repl_offset int
	
	state       ServerState
	is_replica bool

	pollFds     map[int]unix.PollFd	//!Fd to unix polling objs
	clients     map[int]*net.TCPConn
	database    map[string]ValueTickPair
	database_stream map[string]StreamValue
	database_stream_xread_fxns map[string]StreamCallback
	//!Req, response
	requestResponseBuffer map[int][]byte	//!Fd tp response
	forwardingReqBuffer map[int][]byte //!Fd to forwarding req
	cmdProcessed int
	timers                []Timer

	rdbDirPath string
	rdbFileName string
}

// RequestHandler processes the input and returns the response based on server state
func (server *RedisServer) RequestHandler(reqData [][]byte, reqSize int, clientConn *net.TCPConn) ([]byte, error) {

	//!Handle request on the basis of if it is a replica or master.
	var response []byte
	var err error

	cmdName := strings.ToLower(string(reqData[0]));
	switch cmdName {

	//	------------------------------------------------------------------------------------------  //
	case "ping":
		out := "+" + "PONG" + "\r\n"
		response = []byte(out)
		if server.is_replica {
			rs := server.state.(*ReplicaState)
			fmt.Println("Got this from: ", rs.masterFd, " ", rs.masterConn == clientConn)
			response = make([]byte, 0)
		}

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
	case "type":
		if(len(reqData) < 2) {
			fmt.Println("Minimum args req are 2")
		} else {
			var out string
			vt, okd := server.database[string(reqData[1])]		
			_, oks := server.database_stream[string(reqData[1])]
			if okd {
				out = "+" + vt.keyType + "\r\n"
			} else if oks {
				out = "+" + "stream" + "\r\n"
			} else {
				out = "+" + "none" + "\r\n"
			}
			response = []byte(out)
		}

	//	------------------------------------------------------------------------------------------  //		
	case "xrange":
		if(len(reqData) < 4) {
			fmt.Println("Minimum args req are 4")
		} else {
			var out string
			start := string(reqData[2])
			if start == "-" {
				start = "0"
			}
			end := string(reqData[3])
			if end == "+" {
				tend := time.Now().UnixNano() / int64(time.Millisecond) + 100000
				end = strconv.Itoa(int(tend))
			}

			v, ok := server.database_stream[string(reqData[1])]
			if !ok {
				fmt.Print("Key not present...")
				//!We should not encounter this in this case.
			} else {

				var inRangeEntries []StreamEntry
				for _, entry := range v.entries {
					if(entry.id >= start && entry.id <= end) {
						inRangeEntries = append(inRangeEntries, entry)
					}
				}
				if len(inRangeEntries ) != 0 {
					out = createRSEPOutputForStreamValue(inRangeEntries)
				}
			}
			response = []byte(out)
		}

	case "xread":

		var blockIndex, keysIndex, timeIndex int
		blockIndex = -1;

		for i, elem := range reqData {
			if i == 0 {
				continue
			}
			se := string(elem)

			if(strings.ToLower(se) == "block") {
				blockIndex = i;
			}
			if(se == "streams") {
				keysIndex = i + 1
			} 
			
			if strings.Contains(se, "-") {
				timeIndex = i
				break
			}
		}

		timeout := 0		
		if blockIndex != -1 {
			timeout, _ = strconv.Atoi(string(reqData[blockIndex + 1]))
		}

		var allKeys []string
		for i := keysIndex; i < timeIndex; i++ {
			currK := string(reqData[i])
			allKeys = append(allKeys,  currK)
		}

		var allStartTimes []string
		if string(reqData[timeIndex]) == "$" {
			for _, k := range allKeys {
				ls := len(server.database_stream[k].entries) - 1
				lastOne := server.database_stream[k].entries[ls].id
				allStartTimes = append(allStartTimes, lastOne)
			}
		} else {
			for i := timeIndex; i < len(reqData); i++ {
				currTime := string(reqData[i])
				start := currTime
				allStartTimes = append(allStartTimes, start)
			}	
		}

		callbackFunc := func() {
			fmt.Println("Started to execute the callback of xread: ")
			var outSteamWise []string
			tend := time.Now().UnixNano() / int64(time.Millisecond) + 100000
			end := strconv.Itoa(int(tend))
			nothingFound := true
			for i:= 0; i < len(allKeys); i++ {
				currK := allKeys[i]
				start := allStartTimes[i]
				fmt.Println("CK: ", currK, " start time: ",start)
	
				var streamOut string
				v, ok := server.database_stream[currK]
				if !ok {
					fmt.Print("Key not present... in xread, just skipping it for now")
					continue
					//!We should not encounter this in this case.
				} else {
					var inRangeEntries []StreamEntry
					for _, entry := range v.entries {
						if(entry.id > start && entry.id <= end) {
							inRangeEntries = append(inRangeEntries, entry)
						}
					}
					if len(inRangeEntries ) != 0 {
						nothingFound = false						
						streamOut = createRSEPOutputForStreamValue(inRangeEntries)
					}
				}
				keyString := createBulkString(currK)
				currKeyOut := []string{keyString, streamOut}
				kvout := createRESPArray(currKeyOut)
				outSteamWise = append(outSteamWise, kvout)
			}
			var out string
			if nothingFound {
				out = "$-1\r\n"
			} else {
				out = createRESPArray(outSteamWise)
			}
			response = []byte(out)	
			clientConn.Write(response)
		}

		if blockIndex == -1 {
			fmt.Println("No block index provided hence adding with the timer of 0 which means it will trigger it immediately.")
			server.AddTimer(time.Duration(timeout) * time.Millisecond, callbackFunc)			
		} else {
			if timeout == 0 {
				//!Wait for new entryies and write to output, only when something new pops up
				fmt.Println("Input timeout provided is: ", timeout, " with block as parameter")
				for _, k := range allKeys {
					server.database_stream_xread_fxns[k] = StreamCallback{callbackFunc, allKeys}
				}
			} else {
				fmt.Println("timeout provided is: ", timeout, "and to AddTimerFxn internally: ", time.Duration(timeout) * time.Millisecond)
				server.AddTimer(time.Duration(timeout) * time.Millisecond, callbackFunc)				
			}
		}

		//	------------------------------------------------------------------------------------------  //		
	default:
		response, err = server.state.HandleRequest(reqData, reqSize, server, clientConn)
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
		database_stream: make(map[string]StreamValue),
		requestResponseBuffer: make(map[int][]byte),
		forwardingReqBuffer: make(map[int][]byte),	
		timers: make([]Timer, 0),
		database_stream_xread_fxns: make(map[string]StreamCallback),
		rdbDirPath:   rdbDicPath,
		rdbFileName:  rdbFilePath,
		master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		master_repl_offset: 0,
		cmdProcessed: 0,
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
			replicasOffset: make(map[int]int),
		}
		server.is_replica = false
	} else {

		//!Establish the connection to master here, doing the three step handshake, and if we get any data on socket, process it

		server.state = &ReplicaState{
			masterAddress: masterAddress,
		}
		server.is_replica = true
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


//!Adding the support of timer to my event loop
type Timer struct {
	expiry  time.Time
	callback func()
}

// Add a timer to the server's timer list
func (server *RedisServer) AddTimer(duration time.Duration, callback func()) {
	expiry := time.Now().Add(duration)
	server.timers = append(server.timers, Timer{expiry: expiry, callback: callback})
}

// Check and execute expired timers
func (server *RedisServer) processTimers() {
	now := time.Now()
	activeTimers := make([]Timer, 0, len(server.timers))
	if len(server.timers) > 0 {
		//fmt.Println("Inside the process timer. No of timers are: ", server.timers)
	}


	for _, timer := range server.timers {
		if timer.expiry.Before(now) {
			fmt.Println("Timer expired: ", timer)
			timer.callback() // Execute the callback
		} else {
			activeTimers = append(activeTimers, timer)
		}
	}
	server.timers = activeTimers // Keep only non-expired timers
}



func (server *RedisServer) eventLoopStart() {
	fmt.Println("Inside event loop")
	fmt.Println("PollFds on event loop start: ", server.pollFds)

	// Main event loop
	for {	
		//fmt.Println("Iteration")

		//!Creating a poll fd slice
		var pollFdsSlice []unix.PollFd;
		for _, pfd := range server.pollFds {
			pollFdsSlice = append(pollFdsSlice, pfd);
		}


		//!Determine the timeout for polling,
		timeout := -1 // Default: wait indefinitely for I/O events
		if len(server.timers) > 0 {
			now := time.Now()
			nearestTimer := server.timers[0].expiry
			for _, timer := range server.timers {
				if timer.expiry.Before(nearestTimer) {
					nearestTimer = timer.expiry
				}
			}
			timeToNextTimer := nearestTimer.Sub(now)
			if timeToNextTimer < 0 {
				timeToNextTimer = 0
			}
			timeout = int(timeToNextTimer.Milliseconds())
		}
	
		//! Poll for events
		n, err := unix.Poll(pollFdsSlice, timeout) // Wait for I/O events or timeout
		// Poll for events
		//n, err := unix.Poll(pollFdsSlice, -1) // Wait indefinitely for I/O events
		if err != nil {
			fmt.Println("Error polling:", err)
			break
		}


		//! Process expired timers
		server.processTimers()

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
						//fmt.Println("Checking for data: ",string(buffer[:n]))
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
								fmt.Println("Forwarding following bytes: ", string(buffer[:n]))								
								server.state.ForwardRequest(buffer[:n], server)	//!Forwarding the req to all replicas in raw byte forms
							}
							outbytes, _ := server.RequestHandler(inpCmd, inpCmdsSize[currCmdIndex], clientConn)
							//fmt.Println("Curr cmd size: ", inpCmdsSize[currCmdIndex])
							if server.is_replica {
								server.master_repl_offset += inpCmdsSize[currCmdIndex]
							}
							
							if len(outbytes) != 0 {
								server.requestResponseBuffer[fd] = append(server.requestResponseBuffer[fd], outbytes...)
							}
						}
					}
				}

				// Check for write events (socket is ready to send data)
				if pollFdsSlice[i].Revents & unix.POLLOUT != 0 {
					if data, ok := server.requestResponseBuffer[fd]; ok && len(data) > 0 {
						fmt.Println("Sending out: ", string(server.requestResponseBuffer[fd]))
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