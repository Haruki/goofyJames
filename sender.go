package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/golang/protobuf/proto"
)

func main() {
	// Connect to the server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error connecting:", err)
		os.Exit(1)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)

	// Create key-value pairs
	for {
		// Read user input
		fmt.Print("Enter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		key := scanner.Text()

		fmt.Print("Enter value: ")
		scanner.Scan()
		value := scanner.Text()

		// Create a KeyValue message
		kv := &KeyValue{
			Key:   key,
			Value: value,
		}

		// Serialize the message to bytes
		data, err := proto.Marshal(kv)
		if err != nil {
			fmt.Println("Error marshaling:", err)
			os.Exit(1)
		}

		// Write the length of the serialized message
		length := len(data)
		err = writeInt(writer, length)
		if err != nil {
			fmt.Println("Error writing length:", err)
			os.Exit(1)
		}

		// Write the serialized message
		_, err = writer.Write(data)
		if err != nil {
			fmt.Println("Error writing data:", err)
			os.Exit(1)
		}

		// Flush the writer's buffer to send the data immediately
		err = writer.Flush()
		if err != nil {
			fmt.Println("Error flushing:", err)
			os.Exit(1)
		}
	}
}

// Helper function to write an integer as 4 bytes
func writeInt(writer *bufio.Writer, value int) error {
	b := []byte{
		byte(value),
		byte(value >> 8),
		byte(value >> 16),
		byte(value >> 24),
	}
	_, err := writer.Write(b)
	return err
}
