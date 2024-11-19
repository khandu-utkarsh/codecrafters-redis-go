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
	nextIndex := endIndex + 1;
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
	nextIndex := endIndex + 1;
	return currCmd, nextIndex
}

func (server *RedisServer) getCmdFromFile(input []byte)([]byte, int) {
	//fmt.Println("Input: ", len(input), " : ", string(input))
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	//fmt.Println("len: ", len(input), " end index: ", endIndex)
	strElem := string(input[1:endIndex])
	input = input[ endIndex  +2 : ]
	nextIndex := endIndex + 2;
	//fmt.Println("len: ", len(input), " n2 index: ", nextIndex)
	count, _ := strconv.Atoi(strElem)
	//fmt.Println("count: ", count, " n2 index: ", nextIndex)
	cmd := input[: count]
	nextIndex += count;
	//fmt.Println("Current len: ", len(input), " next index: ", nextIndex)
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
			currCmd, br := server.getCmdFromSimpleStringOrInteger(input)
			nextIndex +=  br;
			input = input[nextIndex + 1 :]
			elems[i] = currCmd
		case ':': // Integer
			currCmd, br := server.getCmdFromSimpleStringOrInteger(input)
			nextIndex +=  br;
			input = input[nextIndex  + 1:]
			elems[i] = currCmd
			fmt.Println("Inp rec as interger, look into this once")
		case '$': // Bulk string
			currCmd, br := server.getCmdFromBulkString(input)
			nextIndex += br;
			input = input[nextIndex + 1:]
			elems[i] = currCmd
			fmt.Println("Inp rec as bulk, look into this once")
		default:
			fmt.Println("couldn't figure out what to do here...")			
		}
	}
	return elems, nextIndex;
}

// parseInput validates and parses the input into a structured format
func (server *RedisServer) getCmdsFromInput(inp []byte) ([][][]byte) {
	var commands [][][]byte

	input := inp
	temp := input
	for nextIndex := 0; nextIndex <  len(input); {
		fmt.Println("B Inp len: ", len(input), " next idx: ", nextIndex, " | ", string(input))

		curr := temp[0]
		//!These are three cases considering till now:
		switch curr {
			case '+':	//!This could be the simple string
				currCmd, br := server.getCmdFromSimpleStringOrInteger(temp)
				cmdArray := make([][]byte, 1);
				cmdArray[0] = currCmd;
				nextIndex +=  br;
				commands = append(commands, cmdArray)
				//fmt.Println("Lo: + ", string(currCmd))
			case '$':	//!This could be the file or a bulk string. Asumming that clients always send array, so if we get this, then this must be the file
				currCmd, ni := server.getCmdFromFile(temp)
				cmdArray := make([][]byte, 1);
				cmdArray[0] = currCmd;
				nextIndex +=  ni;
				commands = append(commands, cmdArray)
				//fmt.Println("Lo: $ ", string(currCmd))
				case '*':	//!This could be the array
				cmds, ni := server.getCmdsFromRESPArray(temp)
				nextIndex +=  ni;
				commands = append(commands, cmds)
				//fmt.Println("Lo: * ", cmds)
			default:
				fmt.Println("Encountered something funny: ", string(curr));
		}
		if(nextIndex + 1 == len(temp)) {
			break;
		} else {
			temp = input[nextIndex + 1 : ]
		}

	}
	//fmt.Println("Inp len: ", len(input), " next idx: ", nextIndex)		

	return commands
}
