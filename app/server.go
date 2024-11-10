package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)


type ByteIntPair struct {
    Data []byte
    Value int	//!1 for bulk string, 2 for integer, 3 for simple string
}



func parseInpArray (inputRawData []byte) []ByteIntPair {

	var result []ByteIntPair;

	//!Create a copy:
	input := make([]byte, len(inputRawData))
	copy(input, inputRawData)

	//!Making sure it is an array
	firstByte := input[0];
	if(firstByte != '*') {
		fmt.Println("Wrongly interpretted as array. First character here is: ", firstByte);		
	}	
	crlfSubstr := []byte("\r\n");

	endIndex := bytes.Index(input, crlfSubstr);
	elemsCount, _ := strconv.Atoi(string(input[1: endIndex]));	
	input = input[endIndex + 2 : ];

	// Parse each element
	for currElemIndex := 0; currElemIndex < elemsCount; currElemIndex++ {
		firstByte := input[0];
		switch firstByte {
		case '+':
			endIndex = bytes.Index(input, crlfSubstr);
			result = append(result, ByteIntPair{Data: input[1: endIndex], Value: 1})
		case ':':	//!Integer
			endIndex = bytes.Index(input, crlfSubstr);
			result = append(result, ByteIntPair{Data: input[1: endIndex], Value: 2})
		case '$':	//!Bulk string
			endIndex = bytes.Index(input, crlfSubstr);
			strElem := string(input[1: endIndex]);
			_, _ = strconv.Atoi(strElem)
			input = input[endIndex +2  :]
			endIndex = bytes.Index(input, crlfSubstr);
			//fmt.Println("string is : ", string(input[: endIndex]));
			result = append(result, ByteIntPair{Data: input[: endIndex], Value: 1})
		default:
			fmt.Println("This case has not been yet implemented. First byte is: ", string(input[0]));
			os.Exit(1);
		}
		if(currElemIndex + 1 != elemsCount) {
			input = input[endIndex +2 :]
		}
	}
	return result;
}

func interpretParsedInput (pi []ByteIntPair) ([]ByteIntPair){

	var out []ByteIntPair;
	var i int = 0;

	for i < len(pi) {
		currElem := pi[i];
		if(currElem.Value == 1) {
			cmdName := strings.ToLower(string(currElem.Data))
			if(cmdName == "ping") {
				//!Get the next element and write it to buffer
				out = append(out, ByteIntPair{Data:  []byte("PONG"), Value: 3});
				i++;
			}else if(cmdName == "echo") {
				//!Get the next element and write it to buffer
				outString := string(pi[i+ 1].Data);
				out = append(out, ByteIntPair{Data:  []byte(outString), Value: 1});
				i = i + 2;
			} else if(cmdName == "set") {
				fmt.Println("set not implemented")

			} else if(cmdName == "get") {
				fmt.Println("get not implemented")
			}
		} else if(currElem.Value == 2) {
			fmt.Println("Not implemented for integer")
		}
	}
	return out;
}

func convertIntoRESP(inp []ByteIntPair) []byte{
	// elemCount := len(inp);
	var out string;
	// out = "*"  + strconv.Itoa(elemCount) + "\r\n";

	for _, elem := range inp {
		if(elem.Value == 1) {
			c := len(elem.Data);
			out += "$" + strconv.Itoa(c) + "\r\n" + string(elem.Data) + "\r\n"
		} else if(elem.Value == 2) {
			fmt.Println("No implemented for integer");
		} else if (elem.Value == 3) {
			out += "+" + string(elem.Data) + "\r\n"			
		} else {
			fmt.Println("No implementation for general case as of now");
		}
	}
	return []byte(out);
}


func convertIntoArrayRESP(inp []ByteIntPair) []byte{
	elemCount := len(inp);
	var out string;
	out = "*"  + strconv.Itoa(elemCount) + "\r\n";

	for _, elem := range inp {
		if(elem.Value == 1) {
			c := len(elem.Data);
			out += "$" + strconv.Itoa(c) + "\r\n" + string(elem.Data) + "\r\n"
		} else if(elem.Value == 2) {
			fmt.Println("No implemented for integer");
		} else {
			fmt.Println("No implementation for general case as of now");
		}
	}
	return []byte(out);
}


// GetTCPListenerFd takes a *net.TCPListener and returns its underlying file descriptor.
func GetTCPListenerFd(listener *net.TCPListener) (uintptr, error) {
	// Use SyscallConn to get the underlying connection
	conn, err := listener.SyscallConn()
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

func main() {
    // Create a TCP listener
    listener, err := net.Listen("tcp", ":6379")
    if err != nil {
        fmt.Println("Error listening on port 6379:", err)
        os.Exit(1)
    }
    defer listener.Close()

    // The map of active client connections
    clients := make(map[int]net.Conn)
    pollFds := make(map[int]unix.PollFd)
	requestBuffer := make(map[int][]byte)

 	// Add listener FD to pollFds for read events
	tcpListener := listener.(*net.TCPListener) // Type assertion to *net.TCPListener
	listenerFd, _ := GetTCPListenerFd(tcpListener)
	lfd := int(listenerFd);
    pollFds[lfd] = unix.PollFd{
        Fd:     int32(lfd),
        Events: unix.POLLIN, // We want to read new connections
    }

	fmt.Print("Before executing main loop, map of pollFds: ")	//!For debugging
	fmt.Println(pollFds)

    // Main event loop
    for {
        // Prepare PollFd slice from the map for polling
        var pollFdsSlice []unix.PollFd
        for _, pfd := range pollFds {
            pollFdsSlice = append(pollFdsSlice, pfd)
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
                conn, err := listener.Accept()
                if err != nil {
                    fmt.Println("Error accepting connection:", err)
                    continue
                }

                // Add new connection to pollFds for read/write events
	 			tcpconn := conn.(*net.TCPConn)
	 			tcpconnFd, _ := GetTCPConnectionFd(tcpconn)
				connFd := int(tcpconnFd)
                clients[connFd] = conn
                pollFds[connFd] = unix.PollFd{
                    Fd:     int32(connFd),
                    Events: unix.POLLIN | unix.POLLOUT, // Read and write events
                }

                fmt.Println("Accepted new connection:", conn.RemoteAddr())
				//fmt.Print(pollFds)
            } else {
                // Handle data on an existing connection
                clientConn, ok := clients[fd]
                if !ok {
                    continue
                }

                // Check for read events (data from user)
                if pollFdsSlice[i].Revents&unix.POLLIN != 0 {
                    buffer := make([]byte, 1024)
                    n, err := clientConn.Read(buffer)
					//fmt.Print("Read data is:", string(buffer))
                    if err != nil {
                        fmt.Println("Closing fd: ",fd, " |error: ", err.Error())
                        clientConn.Close()						
                        delete(clients, fd)
                        delete(pollFds, fd) // Remove from pollFds
                    } else {
						if(string(buffer) == "PING") {
							requestBuffer[fd] = append(requestBuffer[fd], []byte("+PONG\r\n")...)	
						} else {
							parsedInput := parseInpArray (buffer[:n])
							outBytePair := interpretParsedInput (parsedInput)
							outByte := convertIntoRESP(outBytePair);
							requestBuffer[fd] = append(requestBuffer[fd], outByte...)	
						}
                 }
                }

                // Check for write events (socket is ready to send data)
                if pollFdsSlice[i].Revents&unix.POLLOUT != 0 {
					if data, ok := requestBuffer[fd]; ok && len(data) > 0 {
						// Write data back to the client (mock response)
						//clientConn.Write([]byte("+PONG\r\n"))
						clientConn.Write(requestBuffer[fd]);
						// Once written, remove the data from the buffer
						delete(requestBuffer, fd)
					}
                }
            }
        }
    }
}