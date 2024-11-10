package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
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
		out += "$" + strconv.Itoa(len(obytes)) + "\r\n" + string(obytes) + "\r\n"
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
				if vt.tickUnixNanoSec  == -1 || vt.tickUnixNanoSec < currTime {
					out += "$" + strconv.Itoa(len(vt.value)) + "\r\n" + vt.value + "\r\n"
				} else {
					fmt.Println("Current time: ", currTime);
					fmt.Println("vt.timeout: ", vt.tickUnixNanoSec)
					out += "$-1\r\n"					
				}
			} else {
				out += "$-1\r\n"
			}
		}

	default:
		fmt.Println("Not yet implemented for, ", cmdName);
	}
	return []byte(out), nil;
		
}


func (server *RedisServer) eventLoopStart() {
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



func main() {

	server, err := NewRedisServer(":6379")
    if err != nil {
        fmt.Println("Error listening on port 6379:", err)
        os.Exit(1)
    }
    defer server.listener.Close()
	server.eventLoopStart();
}