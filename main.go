package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type ApiKey struct {
	Apikey     int
	MinVersion int
	MaxVersion int
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected:", conn.RemoteAddr())
	buf := make([]byte, 4)
	for {
		_, err := io.ReadFull(conn, buf)
		if err != nil {
			fmt.Println("Client Disconnected", conn.RemoteAddr())
			return
		}
		size := binary.BigEndian.Uint32(buf)
		fmt.Println("Request Size:", size)
		msg := make([]byte, size)
		_, err = io.ReadFull(conn, msg)
		if err != nil {
			fmt.Println("Error reading message from client", err)
		}
		apiKey := binary.BigEndian.Uint16(msg[:2])
		apiVersion := binary.BigEndian.Uint16(msg[2:4])
		correlationId := binary.BigEndian.Uint32(msg[4:8])
		fmt.Println("Api key: ", apiKey)
		fmt.Println("Api version: ", apiVersion)
		fmt.Println("Correlation ID: ", correlationId)
		if apiKey == 18 {
			//this is not related to the apiVersion part of the request
			//it is a type of request defined in kafka where apiKey = 18
			fmt.Println("Received ApiVersions Request")
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, int32(correlationId))
			binary.Write(buf, binary.BigEndian, int16(0))

			apiKeys := []struct {
				ApiKey     int
				MinVersion int
				MaxVersion int
			}{
				{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
			}
			apiKeyLen := len(apiKeys)

			binary.Write(buf, binary.BigEndian, int32(apiKeyLen))
			for i := 0; i < apiKeyLen; i++ {
				binary.Write(buf, binary.BigEndian, int16(apiKeys[i].ApiKey))
				binary.Write(buf, binary.BigEndian, int16(apiKeys[i].MinVersion))
				binary.Write(buf, binary.BigEndian, int16(apiKeys[i].MaxVersion))
			}
			size := buf.Len()
			frameByte := new(bytes.Buffer)
			binary.Write(frameByte, binary.BigEndian, int32(size))
			_, err = conn.Write(frameByte.Bytes())
			if err != nil {
				fmt.Println("Error writing to client", err)
				return
			}
			_, err = conn.Write(buf.Bytes())
			if err != nil {
				fmt.Println("Error writing to client", err)
				return
			}
			continue
		}
		res := make([]byte, 8)
		binary.BigEndian.PutUint32(res[:4], 4)
		binary.BigEndian.PutUint32(res[4:], correlationId)
		_, err = conn.Write(res)
		if err != nil {
			fmt.Println("Error writing to client", err)
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
