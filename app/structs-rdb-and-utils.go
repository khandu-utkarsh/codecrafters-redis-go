package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ValueTickPair for value and time pair
type ValueTickPair struct {
	value           string
	tickUnixNanoSec int64
	keyType	string
}

type FundamentalStreamEntry struct {
	key string
	value string
}

type StreamEntry struct {
	id string
	kvpairs []FundamentalStreamEntry
}


type StreamValue struct {
	entries []StreamEntry
}

// GetTCPListenerFd takes a *net.TCPListener and returns its underlying file descriptor.
func GetTCPListenerFd(tcpListener *net.TCPListener) (int, error) {
	conn, err := tcpListener.SyscallConn()
	if err != nil {
		return 0, err
	}

	var fd uintptr
	err = conn.Control(func(fdes uintptr) {
		fd = fdes
	})
	if err != nil {
		return 0, err
	}

	return int(fd), nil
}

// GetTCPConnectionFd takes a *net.TCPConn and returns its underlying file descriptor.
func GetTCPConnectionFd(tcpConn *net.TCPConn) (int, error) {
	conn, err := tcpConn.SyscallConn()
	if err != nil {
		return 0, err
	}

	var fd uintptr
	err = conn.Control(func(fdes uintptr) {
		fd = fdes
	})
	if err != nil {
		return 0, err
	}
	return int(fd), nil
}

// RDBFileManager handles Redis Database (RDB) file parsing.
type RDBFileManager struct {
	directoryPath string
	dBName        string
}

// ParseEncodedLength parses length-prefixed data, supporting multiple formats.
func (rdb *RDBFileManager) ParseEncodedLength(data []byte) (int64, int64) {
	var encodedLength int64
	var bytesRead int64

	firstByte := data[0]
	encodedVal := int64(firstByte) >> 6

	switch encodedVal {
	case 0:
		encodedLength = int64(firstByte & 0x3F)
		bytesRead = int64(1)
	case 1:
		encodedLength = int64(firstByte&0x3F)<<8 | int64(data[1])
		bytesRead = int64(2)
	case 2:
		encodedLength = int64(binary.BigEndian.Uint32(data[1:5])) // Confirm endian system
		bytesRead = int64(4)
	default:
		fmt.Println("Unhandled encoding type:", encodedVal)
	}
	return encodedLength, bytesRead
}

// ParseEncodedString parses encoded string data.
func (rdb *RDBFileManager) ParseEncodedString(data []byte) (string, int64) {
	length, bytesRead := rdb.ParseEncodedLength(data)
	var datstr string
	var totalBytesRead int64
	totalBytesRead = int64(0)
	totalBytesRead += bytesRead
	if length == 0 {
		return datstr, totalBytesRead
	}
	datstr = string(data[totalBytesRead : totalBytesRead+length])
	return datstr, totalBytesRead + length
}

// ParseDictEntry extracts a dictionary entry, including key and value with expiration.
func (rdb *RDBFileManager) ParseDictEntry(data []byte) (string, ValueTickPair, int64) {
	var bytesRead int64 = 1
	var key string
	var vt ValueTickPair

	valueType := data[0]
	if valueType != 0 {
		fmt.Println("Unsupported value type, expecting string.")
		return "", vt, bytesRead
	}

	strKey, br1 := rdb.ParseEncodedString(data[1:])
	strValue, br2 := rdb.ParseEncodedString(data[1+br1:])
	bytesRead += br1 + br2

	key = strKey
	vt.value = strValue
	return key, vt, bytesRead
}

// ParseEntry processes individual entries in the hash table, including expiration.
func (rdb *RDBFileManager) ParseEntry(buffer []byte, server *RedisServer) int64 {
	var expiration int64
	i := int64(0)
	switch buffer[i] {
	case 0xFD:
		expiration = int64(binary.LittleEndian.Uint32(buffer[1:5])) * 1e9
		i += 5
	case 0xFC:
		expiration = int64(binary.LittleEndian.Uint64(buffer[1:9])) * 1e6
		i += 9
	default:
		expiration = -1
	}

	key, vt, bytesRead := rdb.ParseDictEntry(buffer[i:])
	vt.tickUnixNanoSec = expiration
	vt.keyType = "string"
	i += bytesRead
	server.database[key] = vt
	fmt.Println("parsed entry | key:", key, " value:", vt.value, " expiry in ns:", vt.tickUnixNanoSec)
	return i
}

// LoadDatabase loads the database from the specified file.
func (rdb *RDBFileManager) LoadDatabase(server *RedisServer) {
	filePath := filepath.Join(rdb.directoryPath, rdb.dBName)
	rdbFile, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer rdbFile.Close()

	var buffer []byte
	readPosition := int64(0)
	const chunkSize int64 = 1024

	rawData := make([]byte, chunkSize)
	n, err := rdbFile.ReadAt(rawData, readPosition)
	if n == 0 {
		fmt.Println("Error reading file:", err)
		return
	}
	buffer = rawData[:n]

	var index int64
	for ri, currByte := range buffer {
		if currByte == 0xFE {
			index = int64(ri)
			break
		}
	}

	currByte := buffer[index]

	if currByte == 0xFE {
		index++
		encodedLen, bytesRead := rdb.ParseEncodedLength(buffer[index:])
		fmt.Println("Database selector is:", encodedLen, "Bytes read are:", bytesRead)
		index += bytesRead
		currByte = buffer[index]
	}

	if currByte == 0xFB {
		index++
		hashTableSize, bytesRead := rdb.ParseEncodedLength(buffer[index:])
		index += bytesRead
		expiringHashTableSize, bytesRead2 := rdb.ParseEncodedLength(buffer[index:])
		fmt.Println("Read the hash table: hts:", hashTableSize, "expiring hash table:", expiringHashTableSize)
		index += bytesRead2
	}

	for currByte != 0xFE && currByte != 0xFF {
		br := rdb.ParseEntry(buffer[index:], server)
		index += br
		currByte = buffer[index]
	}
}

func splitSteamId(inp string) (int, int) {

	dashIndex := strings.Index(inp, "-")
	if dashIndex == -1 {
		fmt.Println("What is this stream id: ", inp);
		return 0, 0	//!This is error
	}

	timeMillisecond := inp[:dashIndex]
	sqNo := inp[dashIndex + 1: ]

	timeInInt, _ := strconv.Atoi(timeMillisecond)
	seqNoInt, _ := strconv.Atoi(sqNo)
	return timeInInt, seqNoInt
}

func validateString(inp string, last string) (bool, string) {	
	it, is := splitSteamId(inp)

	if(inp == "0-0") {
		return false, "-ERR The ID specified in XADD must be greater than 0-0\r\n"
	}
	lt, ls := splitSteamId(last)
	if(it > lt) {
		return true, ""
	} else if(it == lt) {
		if(is > ls) {
			return true, ""
		} else {
			return false, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"	
		}
	}
	return false, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
	

}


func generateId(inp string, last string) (bool, string) {	
	if !strings.Contains(inp, "*") {
		return false, inp
	}
	lt, ls := splitSteamId(last)

	var newTime int
	var newseq int	

	dashIndex := strings.Index(inp, "-")
	var inpt int
	if dashIndex == -1 {
		inpt = int(time.Now().UnixNano() / int64(time.Millisecond))
	} else {
		inpt, _ = strconv.Atoi(inp[:dashIndex])
	}		
	newTime = inpt
	if inpt == lt {
		newseq = ls + 1
	} else {
		newseq = 0;
	}
	return true, strconv.Itoa(newTime) + "-" + strconv.Itoa(newseq)
}