package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/golang/protobuf/proto"
)

func main() {
	// Start a TCP server
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Println("Server is running and listening on port 8080")

	for {
		// Accept incoming connections
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			os.Exit(1)
		}

		// Handle the connection in a goroutine
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// Read the length of the incoming message
		length, err := readInt(reader)
		if err != nil {
			fmt.Println("Error reading length:", err)
			return
		}

		// Read the serialized message
		data := make([]byte, length)
		_, err = reader.Read(data)
		if err != nil {
			fmt.Println("Error reading data:", err)
			return
		}

		// Deserialize the message
		kv := &KeyValue{}
		err = proto.Unmarshal(data, kv)
		if err != nil {
			fmt.Println("Error unmarshaling:", err)
			return
		}

		if kv.Key == "" && kv.Value == "" {
			// Exit the loop if an empty KeyValue message is received
			return
		}

		// Process the received key-value pair
		fmt.Printf("Received: %s = %s\n", kv.Key, kv.Value)
	}
}

// Helper function to read an integer from 4 bytes
func readInt(reader *bufio.Reader) (int, error) {
	b := make([]byte, 4)
	_, err := reader.Read(b)
	if err != nil {
		return 0, err
	}
	value := int(b[0]) | int(b[1])<<8 | int(b[2])<<16 | int(b[3])<<24
	return value, nil
}
