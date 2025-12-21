package main

import (
	"fmt"
	"net"
)

func handleConn(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected:", conn.RemoteAddr())
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Client disconnected", conn.RemoteAddr())
			return
		}

		_, err = conn.Write([]byte("Echo: "))
		if err != nil {
			fmt.Println("Error writing to client")
			return
		}

		_,err = conn.Write(buf[:n])
		if err != nil {
			fmt.Println("Error writing to cleint")
			return
		}

	}
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error Listening in port 8080")
		fmt.Println(err)
		return
	}

	defer lis.Close()

	fmt.Println("server is listening on port 8080...")

	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn)
	}
}
