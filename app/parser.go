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

func createIntegerString(inp int) (string) {
	return ":" + strconv.Itoa(inp) + "\r\n"
}

func createGetAckString() (string) {
	return "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
}


func createRSEPOutputForStreamValue(allse []StreamEntry) (string) {
	var obentrystrings []string
	for _, entry := range allse {
		id := entry.id
		idstring := createBulkString(id)
		kvps := entry.kvpairs
		var kvarrays []string
		for _, kv := range kvps {
			kout := createBulkString(kv.key)
			vout := createBulkString(kv.value)
			kvarrays = append(kvarrays, kout, vout)
		}
		entriesArray := createRESPArray(kvarrays)
		entryString := []string{idstring, entriesArray}
		earray := createRESPArray(entryString)
		obentrystrings = append(obentrystrings, earray)
	}
	output := createRESPArray(obentrystrings)
	return output
}

//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//
//!Input fxns
//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//	//


func (server *RedisServer) getCmdFromSimpleStringOrInteger(input []byte)([]byte, int) {
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	currCmd := input[1 : endIndex];
	br := endIndex + 2;
	return currCmd, br
}

func (server *RedisServer) getCmdFromBulkString(input []byte)([]byte, int) {
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	strElem := string(input[1:endIndex])
	bc, _ := strconv.Atoi(strElem)
	content := input[endIndex + 2: endIndex +2 + bc]
	input = input[endIndex + 2 + bc: ]
	newEndIndex := bytes.Index(input, crlfSubstr)
	if newEndIndex != 0 {
		fmt.Println("something wrong")
	}
	currCmd := content
	return currCmd, endIndex + 2 + bc + 2;
}

func (server *RedisServer) getCmdFromFile(input []byte)([]byte, int) {
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	strElem := string(input[1:endIndex])
	count, _ := strconv.Atoi(strElem)
	cmd := input[ endIndex  +2 : endIndex  +2 + count]
	return cmd, endIndex + 2 + count	//!
}



func (server *RedisServer) getCmdsFromRESPArray(input []byte)([][]byte, int) {

	//fmt.Println("Entered resp array parser: input bytes are: ", string(input))
	crlfSubstr := []byte("\r\n")
	endIndex := bytes.Index(input, crlfSubstr)
	elemsCount, err := strconv.Atoi(string(input[1:endIndex]))
	if err != nil {
		fmt.Println("invalid element count")
		return nil, 0
	}

	totalBr := 0
	totalBr += endIndex + 2;

	elems := make([][]byte, elemsCount)
	input = input[totalBr :]

	for i := 0; i < elemsCount; i++ {
		//fmt.Println("New iteration: ", i, " |Rem input: ", string(input))
		firstByte := input[0]
		bread := 0;
		switch firstByte {
		case '+': // Simple string
			currCmd, br := server.getCmdFromSimpleStringOrInteger(input)
			bread = br;
			elems[i] = currCmd
			case ':': // Integer
			currCmd, br := server.getCmdFromSimpleStringOrInteger(input)
			bread = br;
			elems[i] = currCmd
			//fmt.Println("Inp rec as interger, look into this once")
		case '$': // Bulk string
			currCmd, br := server.getCmdFromBulkString(input)
			bread = br;
			elems[i] = currCmd
			//fmt.Println("Inp rec as bulk, look into this once")
		default:
			fmt.Println("couldn't figure out what to do here... inside decoding resp array")			
		}
		//fmt.Println("Current cmd at iter i: ", i, " is: ", string(elems[i]), " and bytes read are: ", bread)
		totalBr += bread
		if(bread != len(input)) {
			input = input[bread : ]
		}		
	}
	return elems, totalBr;
}

// parseInput validates and parses the input into a structured format
func (server *RedisServer) getCmdsFromInput(inp []byte) ([][][]byte, []int) {
	var commands [][][]byte
	var commandSizes []int

	//fmt.Println("Parsing input | Input len: ", len(inp), " | Input data: ", string(inp))

	breakEarly := false
	for i := 0; len(inp) > 0 && !breakEarly; i++ {
		bytesRead :=  0
		curr := inp[0]
		switch curr {
			case '+':	//!This could be the simple string
				currCmd, br := server.getCmdFromSimpleStringOrInteger(inp)
				cmdArray := make([][]byte, 1)
				cmdArray[0] = currCmd
				bytesRead +=  br
				commands = append(commands, cmdArray)
				commandSizes = append(commandSizes, br)
				//fmt.Println("Lo: + br: ", br, " ", string(currCmd))
			case '$':	//!This could be the file or a bulk string. Asumming that clients always send array, so if we get this, then this must be the file
				currCmd, br := server.getCmdFromFile(inp)
				cmdArray := make([][]byte, 1);
				cmdArray[0] = currCmd
				bytesRead +=  br
				commands = append(commands, cmdArray)
				commandSizes = append(commandSizes, br)
				case '*':	//!This could be the array
				cmds, br := server.getCmdsFromRESPArray(inp)
				bytesRead +=  br
				commands = append(commands, cmds)
				commandSizes = append(commandSizes, br)
				//fmt.Println("Array: * br: ", bytesRead, " ", cmds)
			default:
				fmt.Println("Encountered something funny: ", string(curr));
				breakEarly = true
		}
		if bytesRead != len(inp) {
			inp = inp[bytesRead :]
		} else {
			breakEarly = true
		}
	}
	return commands, commandSizes
}