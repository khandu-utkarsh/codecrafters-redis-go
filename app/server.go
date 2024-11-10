package main

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

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
                    if err != nil {
                        fmt.Println("Closing fd: ",fd, " |error: ", err.Error())
                        clientConn.Close()						
                        delete(clients, fd)
                        delete(pollFds, fd) // Remove from pollFds
                    } else {
						requestBuffer[fd] = append(requestBuffer[fd], buffer[:n]...)
                        //fmt.Printf("Received from client: %s\n", string(buffer[:n]))	//!For debugging
                    }
                }

                // Check for write events (socket is ready to send data)
                if pollFdsSlice[i].Revents&unix.POLLOUT != 0 {
					if data, ok := requestBuffer[fd]; ok && len(data) > 0 {
						// Write data back to the client (mock response)
						clientConn.Write([]byte("+PONG\r\n"))
						// Once written, remove the data from the buffer
						delete(requestBuffer, fd)
					}
                }
            }
        }
    }
}