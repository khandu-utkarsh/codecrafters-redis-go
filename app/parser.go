package main

import (
	"bytes"
	"fmt"
	"strconv"
)

//!Ouput fxns

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

//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//
//!Input fxns
//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//


func (server *RedisServer) getCmdFromSimpleStringOrInteger(input []byte)([]byte, int) {

	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	currCmd := input[1 : endIndex];
	nextIndex := endIndex + 2;
	return currCmd, nextIndex
}

func (server *RedisServer) getCmdFromBulkString(input []byte)([]byte, int) {

	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	strElem := string(input[1:endIndex])
	_, _ = strconv.Atoi(strElem)
	input = input[endIndex+2:]
	endIndex = bytes.Index(input, crlfSubstr)
	currCmd := input[: endIndex];
	nextIndex := endIndex + 2;
	return currCmd, nextIndex
}

func (server *RedisServer) getCmdFromFile(input []byte)([]byte, int) {
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	strElem := string(input[1:endIndex])
	input = input[ endIndex  +2 : ]
	nextIndex := endIndex + 2;
	count, _ := strconv.Atoi(strElem)
	cmd := input[: count]
	nextIndex += count + 1;
	return cmd, nextIndex
}



func (server *RedisServer) getCmdsFromRESPArray(input []byte)([][]byte, int) {

	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	elemsCount, err := strconv.Atoi(string(input[1:endIndex]))
	if err != nil {
		fmt.Println("invalid element count")
		return nil, 0
	}

	elems := make([][]byte, elemsCount)
	nextIndex := endIndex + 2;
	input = input[nextIndex :]

	for i := 0; i < elemsCount; i++ {
		firstByte := input[0]
		switch firstByte {
		case '+': // Simple string
			currCmd, ni := server.getCmdFromSimpleStringOrInteger(input)
			nextIndex =  ni;
			input = input[nextIndex :]
			elems[i] = currCmd
		case ':': // Integer
			currCmd, ni := server.getCmdFromSimpleStringOrInteger(input)
			nextIndex =  ni;
			input = input[nextIndex :]
			elems[i] = currCmd
			fmt.Println("Inp rec as interger, look into this once")
		case '$': // Bulk string
			currCmd, ni := server.getCmdFromBulkString(input)
			nextIndex =  ni;
			input = input[nextIndex :]
			elems[i] = currCmd
			fmt.Println("Inp rec as interger, look into this once")
		default:
			fmt.Println("couldn't figure out what to do here...")			
		}
	}
	return elems, nextIndex;
}

// parseInput validates and parses the input into a structured format
func (server *RedisServer) getCmdsFromInput(inp []byte) ([][][]byte) {
	var commands [][][]byte

	for nextIndex := 0; nextIndex < len(inp); {
		input := inp[nextIndex : ]
		curr := input[0]
		//!These are three cases considering till now:
		switch curr {
			case '+':	//!This could be the simple string
				currCmd, ni := server.getCmdFromSimpleStringOrInteger(input)
				cmdArray := make([][]byte, 1);
				cmdArray[0] = currCmd;
				nextIndex =  ni;
				commands = append(commands, cmdArray)
			case '$':	//!This could be the file or a bulk string. Asumming that clients always send array, so if we get this, then this must be the file
				currCmd, ni := server.getCmdFromFile(input)
				cmdArray := make([][]byte, 1);
				cmdArray[0] = currCmd;
				nextIndex =  ni;
				commands = append(commands, cmdArray)
			case '*':	//!This could be the array
				cmds, ni := server.getCmdsFromRESPArray(input)
				nextIndex =  ni;
				commands = append(commands, cmds)
			default:
				fmt.Println("Encountered something funny: ", string(curr));
		}
	}

	return commands
}
