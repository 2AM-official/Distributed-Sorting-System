package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type Client struct {
	Conn  net.Conn
	Value int
}

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	// variables
	input := os.Args[2]
	output := os.Args[3]
	serverNums := len(scs.Servers)
	var curFile [][]byte

	// build server table to store data
	serverTable := buildTable(input, serverNums)

	// find host and port of the cur server
	var host string
	var port string

	for i := range scs.Servers {
		if scs.Servers[i].ServerId == serverId {
			host = scs.Servers[i].Host
			port = scs.Servers[i].Port
			break
		}
	}

	// listen to ports
	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Printf("unsucessful building listener")
		os.Exit(1)
	}

	// channle storage
	storage := make(chan []byte)

	for i := range scs.Servers {
		if scs.Servers[i].ServerId == serverId {
			continue
		}
		// clientID := scs.Servers[i].ServerId
		clientHOST := scs.Servers[i].Host
		clientPORT := scs.Servers[i].Port
		go recieve(storage, clientHOST, clientPORT, serverId)
	}

	// send files
	counter := 0
	unblockingCh := make(chan bool)
	for {
		if counter == serverNums-1 {
			fmt.Printf("connect to all servers")
			break
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("unsuccessful connection")
			continue
		}
		counter++
		go handleConnection(conn, unblockingCh, serverTable)
	}

	completeClients := 0
	for {
		if completeClients == serverNums-1 {
			fmt.Printf("recieve data from all clients")
			break
		}
		// put channel storage to cur file
		file := <-storage
		completeClients += 1
		size := len(file) / 100
		for i := 0; i < size; i++ {
			curFile = append(curFile, file[i*100:(i+1)*100])
		}
	}

	// write the current file
	for i := 0; i < len(serverTable[serverId]); i++ {
		curFile = append(curFile, serverTable[serverId][i])
	}

	for i := 0; i < serverNums-1; i++ {
		<-unblockingCh
	}

	// sort the data belongs to this server
	sort.Slice(curFile, func(i, j int) bool {
		return bytes.Compare(curFile[i][:10], curFile[j][:10]) < 0
	})

	// write the output file
	//fmt.Println("Hello World")
	ioutil.WriteFile(output, bytes.Join(curFile, []byte("")), 0666)
}

// get server id
func obtainID(lenBit int, data []byte) int {
	counter := 0
	serverID := 0
loop:
	for _, d := range data {
		for i := 7; i >= 0; i-- {
			if counter >= lenBit {
				break loop
			}
			tmp := int(d) & (1 << i) >> i
			serverID = serverID<<1 + tmp
			counter++
		}
	}
	return serverID
}

func buildTable(inputFile string, serverNums int) map[int][][]byte {
	lenBit := int(math.Log2(float64(serverNums)))
	file, err := ioutil.ReadFile(inputFile)
	if err != nil {
		panic(err)
	}
	size := len(file) / 100
	serverTable := make(map[int][][]byte)
	for i := 0; i < size; i++ {
		serverID := obtainID(lenBit, file[i*100:i*100+10])
		serverTable[serverID] = append(serverTable[serverID], file[i*100:(i+1)*100])
		fmt.Printf("%d", serverID)
	}
	return serverTable
}

func recieve(storage chan []byte, host string, port string, curId int) {
	var conn net.Conn
	var err error

	for {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			// wait until dial with the port
			time.Sleep(time.Millisecond * 20)
		} else {
			fmt.Println("connect to ", host)
			break
		}
	}
	defer conn.Close()

	// send id to establish connection
	clientID := []byte(strconv.Itoa(curId))
	_, err = conn.Write(clientID)
	if err != nil {
		panic(err)
	}

	// receive file from servers
	var res []byte
	buffer := make([]byte, 100)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("All data has been received")
				break
			} else {
				log.Printf("Received data failed %v", err)
				continue
			}
		}

		if string(buffer[:n]) == "end" {
			fmt.Println("end is read and all data has been received")
			break
		}
		// fmt.Println("received from", host)
		res = append(res, buffer...)
	}
	storage <- res
}

func handleConnection(conn net.Conn, ch chan bool, serverTable map[int][][]byte) {
	defer conn.Close()

	buffer := make([]byte, 16)
	client, err := conn.Read(buffer)
	if err != nil {
		log.Printf("unsuccessful recieve client id")
	}
	clientID, _ := strconv.Atoi(string(buffer[:client]))
	for ind := 0; ind < len(serverTable[clientID]); ind++ {
		content := serverTable[clientID][ind]
		_, err := conn.Write(content)
		if err != nil {
			log.Printf("unsucessful sending msg")
		}
		//fmt.Println("send msg to ", clientID, "debug content", content)
	}
	fmt.Println("successfully send data to remote")
	conn.Write([]byte("end"))
	ch <- true
}
