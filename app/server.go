package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type ByteIntPair struct {
    Data []byte
    Value int	//!1 for bulk string, 2 for integer, 3 for simple string
}

type ValueTickPair struct {
	value string
	tickUnixNanoSec int64
}

//!Create a reddis server
type RedisServer struct {
	listener    net.Listener
	clients     map[int]net.Conn
	pollFds     map[int]unix.PollFd
	requestResponseBuffer map[int][]byte
	databse map[string]ValueTickPair

	//!Configs for rdb persistence
	rdbDirPath string
	rdbFileName string
}

//!Constructor
func NewRedisServer(address string) (*RedisServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("error listening on port %s: %v", address, err)
	}

	return &RedisServer{
		listener:     listener,
		clients:      make(map[int]net.Conn),
		pollFds:      make(map[int]unix.PollFd),
		requestResponseBuffer: make(map[int][]byte),
		databse:  make(map[string]ValueTickPair),
	}, nil
}


// GetTCPListenerFd takes a *net.TCPListener and returns its underlying file descriptor.
func GetTCPListenerFd(tcpListener *net.TCPListener) (uintptr, error) {
	// Use SyscallConn to get the underlying connection
	conn, err := 	tcpListener.SyscallConn()
	if err != nil {
		return 0, err
	}

	var fd uintptr
	// Use the Control method to retrieve the file descriptor
	err = conn.Control(func(fdes uintptr) {
		fd = fdes
	})
	if err != nil {
		return 0, err
	}

	return fd, nil
}


func GetTCPConnectionFd(tcpConn *net.TCPConn) (uintptr, error) {
	// Use SyscallConn to get the underlying connection
	conn, err := tcpConn.SyscallConn()
	if err != nil {
		return 0, err
	}

	var fd uintptr
	// Use the Control method to retrieve the file descriptor
	err = conn.Control(func(fdes uintptr) {
		fd = fdes
	})
	if err != nil {
		return 0, err
	}
	return fd, nil	
}

func createBulkString(inp string) (string) {
	return "$" + strconv.Itoa(len(inp)) + "\r\n" + string(inp) + "\r\n"
}

func createRESPArray(inparray []string) (string) {
	out := "*";
	out += strconv.Itoa(len(inparray));
	out += "\r\n";
	for _, elem := range inparray {
		out += elem;
	}
	return out;

}

// RequestHandler processes the input and returns the response
func (server *RedisServer) RequestHandler(inputRawData []byte) ([]byte, error) {
	var result []ByteIntPair
	var out string

	// Create a copy of the input
	input := make([]byte, len(inputRawData))
	copy(input, inputRawData)

	// Ensure it is an array
	if input[0] != '*' {
		return nil, fmt.Errorf("invalid format: expected array, found %c", input[0])
	}

	// Find number of elements in the array
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	elemsCount, _ := strconv.Atoi(string(input[1:endIndex]))
	input = input[endIndex+2:]

	// Parse each element in the array
	for i := 0; i < elemsCount; i++ {
		firstByte := input[0]
		switch firstByte {
		case '+': // Simple string
			endIndex = bytes.Index(input, crlfSubstr)
			result = append(result, ByteIntPair{Data: input[1:endIndex], Value: 3})
		case ':': // Integer
			endIndex = bytes.Index(input, crlfSubstr)
			result = append(result, ByteIntPair{Data: input[1:endIndex], Value: 2})
		case '$': // Bulk string
			endIndex = bytes.Index(input, crlfSubstr)
			strElem := string(input[1:endIndex])
			_, _ = strconv.Atoi(strElem)
			input = input[endIndex+2:]
			endIndex = bytes.Index(input, crlfSubstr)
			result = append(result, ByteIntPair{Data: input[:endIndex], Value: 1})
		default:
			return nil, fmt.Errorf("unhandled case: %c", input[0])
		}
		if(i + 1 != elemsCount) {
			input = input[endIndex +2 :]
		}
	}

	if(result[0].Value == 2) {
		fmt.Println("First element in the array encountered to be integer, look into it.")
		return nil, fmt.Errorf("integer as the first element in input array")
	}

	cmdName := strings.ToLower(string(result[0].Data));
	switch cmdName {
	case "ping":
		out += "+" + "PONG" + "\r\n"

	case "echo":
		//!Get byte array of all the rest elements
		var obytes []byte
		for elemIndex, elem := range result {
			if(elemIndex == 0) {
				continue;
			}
			obytes = append(obytes, elem.Data...)
		}
		out += createBulkString(string(obytes));
	case "set":
		if(len(result) < 3) {
			fmt.Println("Minimum args req are 3")
		} else {
			key := string(result[1].Data);
			value := string(result[2].Data);

			//!Check if information of time present
			var vt ValueTickPair;
			vt.value = value;
			if len(result) > 3 {	//!Time included
				timeUnit := strings.ToLower(string(result[3].Data))
				timeDuration, _ := strconv.Atoi(string(result[4].Data))
				if(timeUnit == "px") {
					timeDuration = timeDuration * 1000000;	//!Mili to nano seconds
				} else {
					fmt.Println("Time unit not known");					
				}
				vt.tickUnixNanoSec = time.Now().UnixNano() + int64(timeDuration); 
			} else {
				vt.tickUnixNanoSec = -1;	//!Never expires
			}
			server.databse[key] = vt;
			out += "+" + "OK" + "\r\n"			
		}
	case "get":
		if(len(result) < 2) {
			fmt.Println("Minimum args req are 2")
		} else {
			vt, ok := server.databse[string(result[1].Data)]		
			if ok {
				currTime := time.Now().UnixNano();				
				if vt.tickUnixNanoSec  == -1 || vt.tickUnixNanoSec > currTime {
					out += createBulkString(vt.value);
				} else {
					fmt.Println("Current time: ", currTime);
					fmt.Println("vt.timeout: ", vt.tickUnixNanoSec)
					out += "$-1\r\n"					
				}
			} else {
				out += "$-1\r\n"
			}
		}
	case "config":
		if(len(result) < 3) {
			fmt.Println("Minimum args req are 3 for config")
		} else {
			if strings.ToLower(string(result[1].Data)) == "get" && string(result[2].Data) == "dir" {
				oa := make([]string, 2)
				oa[0] = createBulkString("dir");
				oa[1] = createBulkString(server.rdbDirPath);
				out += createRESPArray(oa);
			} else if strings.ToLower(string(result[1].Data)) == "get" && string(result[2].Data) == "dbfilename" { 
				oa := make([]string, 2)
				oa[0] = createBulkString("dbfilename");
				oa[1] = createBulkString(server.rdbFileName);
				out += createRESPArray(oa);
			} else {
				fmt.Println("Nothing implemented for this value of config");				
			}
		}
	default:
		fmt.Println("Not yet implemented for, ", cmdName);
	}
	return []byte(out), nil;
		
}


func (server *RedisServer) eventLoopStart() {

	//!Before starting the server let's load the rdb file and populate the cache databse.

	rdbManager := &RDBFileManager{
						directoryPath : server.rdbDirPath,
						dbName : server.rdbFileName,
					}

	rdbManager.LoadDatabase(server);

	// Add listener FD to pollFds for read events
	listenerFd, _ := GetTCPListenerFd(server.listener.(*net.TCPListener))
	lfd := int(listenerFd)

	server.pollFds[lfd] = unix.PollFd{Fd: int32(lfd), Events: unix.POLLIN};

	fmt.Print("Before executing main loop, map of pollFds: ")
	fmt.Println(server.pollFds)

	// Main event loop
	for {	

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
	
		// Handle each event
		for i := 0; i < n; i++ {
			fd := int(pollFdsSlice[i].Fd)
			if fd == lfd && pollFdsSlice[i].Revents & unix.POLLIN != 0 {

				// New connection request
				conn, err := server.listener.Accept()
				if err != nil {
					fmt.Println("Error accepting connection:", err)
					continue
				}
	
				// Add new connection to pollFds for read/write events
				tcpConn := conn.(*net.TCPConn)
				tcpConnFd, _ := GetTCPConnectionFd(tcpConn)
				connFd := int(tcpConnFd)

				server.clients[connFd] = conn
				server.pollFds[connFd] = unix.PollFd{ Fd: int32(connFd), Events: unix.POLLIN | unix.POLLOUT,};
				fmt.Println("Accepted new connection:", conn.RemoteAddr())
			} else {

				clientConn, ok := server.clients[fd]
				if !ok {
					continue
				}
	
				// Check for read events (data from user)
				if pollFdsSlice[i].Revents & unix.POLLIN != 0 {
					buffer := make([]byte, 1024)
					n, err := clientConn.Read(buffer)
					if err != nil {
						fmt.Println("Closing fd:", fd, "|error:", err.Error())
						clientConn.Close()
						delete(server.clients, fd)
						delete(server.pollFds, fd) // Remove from pollFds
					} else {
						//fmt.Println("Read data is: ", string(buffer))
						outbytes, _ := server.RequestHandler(buffer[:n])
						server.requestResponseBuffer[fd] = append(server.requestResponseBuffer[fd], outbytes...)
					}
				}

				// Check for write events (socket is ready to send data)
				if pollFdsSlice[i].Revents & unix.POLLOUT != 0 {
					if data, ok := server.requestResponseBuffer[fd]; ok && len(data) > 0 {
						clientConn.Write(server.requestResponseBuffer[fd])
						delete(server.requestResponseBuffer, fd)
					}
				}
			}
		}
	}
}

// RDBFileManager handles Redis Database (RDB) file parsing.
type RDBFileManager struct {
	directoryPath         string
	dbName                string
	databaseSelector      int64
	hashTableSize         int64
	expiringHashTableSize int64
}

// parseEncodedData parses length-prefixed data, supporting multiple formats.
func (rdb *RDBFileManager) parseEncodedData(data []byte) (interface{}, string, int64) {
	var readData interface{}
	var dataType string
	var nextByteIndex int64

	firstByte := data[0]
	encodedVal := int64(firstByte) >> 6

	switch encodedVal {
	case 0: // 6-bit length
		len := int64(firstByte & 0x3F)
		readData = string(data[1 : 1+len])
		dataType = "string"
		nextByteIndex = 1 + len
	case 1: // 14-bit length
		len := int64(firstByte&0x3F)<<8 | int64(data[1])
		readData = string(data[2 : 2+len])
		dataType = "string"
		nextByteIndex = 2 + len
	case 2: // 32-bit length
		len := int64(binary.BigEndian.Uint32(data[1:5]))
		readData = string(data[5 : 5+len])
		dataType = "string"
		nextByteIndex = 5 + len
	case 3: // Integer types
		whatFollows := int64(firstByte & 0x3F)
		readData, dataType, nextByteIndex = rdb.parseIntegerData(data[1:], whatFollows)
	default:
		fmt.Println("Unhandled encoding type.")
	}

	return readData, dataType, nextByteIndex
}

// parseIntegerData handles integer data types based on a type identifier.
func (rdb *RDBFileManager) parseIntegerData(data []byte, dataTypeID int64) (int64, string, int64) {
	var result int64
	var nextByteIndex int64

	switch dataTypeID {
	case 0:
		result = int64(data[0])
		nextByteIndex = 1
	case 1:
		result = int64(binary.BigEndian.Uint16(data[:2]))
		nextByteIndex = 2
	case 2:
		result = int64(binary.BigEndian.Uint32(data[:4]))
		nextByteIndex = 4
	default:
		fmt.Println("Unknown integer type")
	}

	return result, "int64", nextByteIndex
}

// parseDictEntry extracts a dictionary entry, including key and value with expiration.
func (rdb *RDBFileManager) parseDictEntry(data []byte) (string, ValueTickPair, int64) {
	var bytesRead int64 = 1
	var key string
	var vt ValueTickPair

	valueType := data[0]
	if valueType != 0 {
		fmt.Println("Unsupported value type, expecting string.")
		return "", vt, bytesRead
	}

	strKey, _, keyLen := rdb.parseEncodedData(data[bytesRead:])
	bytesRead += keyLen
	key = string(data[bytesRead : bytesRead+strKey.(int64)])
	bytesRead += strKey.(int64)
	strValue, _, valueLen := rdb.parseEncodedData(data[bytesRead: ])
	bytesRead += valueLen
	vt.value = string(data[bytesRead : bytesRead+strValue.(int64)])
	bytesRead += strValue.(int64)

	return key, vt, bytesRead
}

// processDatabaseSelector processes the database selector entry in the RDB file.
func (rdb *RDBFileManager) processDatabaseSelector(data []byte) (int64, int64) {
	parsedData, dataType, nextIndex := rdb.parseEncodedData(data)
	if dataType != "int64" {
		fmt.Println("Database selector should be int64, but got:", dataType)
	}
	return parsedData.(int64), nextIndex
}

// processHashTableSizes extracts the main and expiring hash table sizes.
func (rdb *RDBFileManager) processHashTableSizes(data []byte) (int64, int64, int64) {
	mainSize, _, offset := rdb.parseEncodedData(data)
	expiringSize, _, nextOffset := rdb.parseEncodedData(data[offset:])
	return mainSize.(int64), expiringSize.(int64), offset + nextOffset
}

// parseEntry processes individual entries in the hash table, including expiration.
func (rdb *RDBFileManager) parseEntry(buffer []byte, server *RedisServer) int64 {
	var expiration int64
	i := int64(0);
	switch buffer[i] {
	case 0xFD:
		expiration = int64(binary.BigEndian.Uint32(buffer[1:5])) * 1e9
		i += 5
	case 0xFC:
		expiration = int64(binary.BigEndian.Uint64(buffer[1:9])) * 1e6
		i += 9
	default:
		expiration = -1
	}

	key, vt, bytesRead := rdb.parseDictEntry(buffer[i:])
	vt.tickUnixNanoSec = expiration
	i += bytesRead
	server.databse[key] = vt
	return i
}



func (rdb * RDBFileManager) LoadDatabase(server *RedisServer) {
	filePath := filepath.Join(server.rdbDirPath, server.rdbFileName)
	rdbFile, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Unable to open file at:", filePath)
		return
	}
	defer rdbFile.Close()

	var buffer []byte
	readPosition := int64(0)
	const chunkSize int64 = 1024
	hashtableStarted := false

	for {
		rawData := make([]byte, chunkSize)
		n, err := rdbFile.ReadAt(rawData, readPosition)
		if err != nil {
			fmt.Println("Error reading file:", err)
			break
		}

		buffer = append(buffer, rawData...)
		readPosition += int64(n)

		//!Go over each byte till we encounter database selector
		for i := int64(0); i < int64(len(buffer)); {
			br := int64(0)
			if buffer[i] == 0xFE && !hashtableStarted {
				rdb.databaseSelector, br = rdb.processDatabaseSelector(buffer[i + 1:])
				i += 1 + br
				continue
			}

			if buffer[i] == 0xFB && !hashtableStarted {
				rdb.hashTableSize, rdb.expiringHashTableSize, br = rdb.processHashTableSizes(buffer[i + 1:])
				i += 1 + br
				hashtableStarted = true
				continue
			}

			if hashtableStarted {
				br = rdb.parseEntry(buffer[i:], server)
				i += br;
			}
		}

		if n < int(chunkSize) {
			fmt.Println("File read completed.")
			break
		}
	}


}


func main() {

	cmdArgs := os.Args[1:];

	var dirName string;
	var rdbFileName string;
	for index, arg := range cmdArgs {
		switch arg {
		case "--dir":
			dirName = cmdArgs[index + 1];			
		case "--dbfilename":
			rdbFileName = cmdArgs[index + 1];
		default:
			continue;
		}
	}

	fmt.Println("Printing all the cmd line arguments")
	fmt.Println(cmdArgs)


	server, err := NewRedisServer(":6379")
    if err != nil {
        fmt.Println("Error listening on port 6379:", err)
        os.Exit(1)
    }
	//!Setting the rdb params:
	server.rdbDirPath = dirName;
	server.rdbFileName = rdbFileName;
    defer server.listener.Close()
	server.eventLoopStart();
}