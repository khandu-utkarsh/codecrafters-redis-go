package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	//fmt.Println("Logs from your program will appear here!")

	//Uncomment this block to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}


	//!This returns the persistent connection:
	connection, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer connection.Close();
	
	readBuffer := make([]byte, 1024);

	var allReadData []byte

	for {
		// Read data into the buffer.
		n, err := connection.Read(readBuffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed by peer.")
				break // Gracefully exit when the connection is closed
			}
			fmt.Println("Error reading from connection:", err.Error())
			os.Exit(1);
		}
		retStr := "+PONG\r\n";
		connection.Write([]byte(retStr));
		// Append the received data to allData.
		allReadData = append(allReadData, readBuffer[:n]...);
		_ = allReadData;
	}

	// //!Multiple commands on one connection
	// inpCmds := string(readBytes);
	// inpCmdList := strings.Split(inpCmds, "\n");

	// var retStr string;
	// for range(inpCmdList) {
	// 	retStr += "+PONG\r\n";
	// }
	// connection.Write([]byte(retStr));
}
