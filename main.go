package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

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
			return
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
		//api key 0 => PRODUCE request
		if apiKey == 0 {
			produceReq := msg[8:]

			p := bytes.NewReader(produceReq)

			clientIdLen := must(readInt16(p))
			if clientIdLen != -1 {
				clientId := must(readBytes(p, int32(clientIdLen)))
				fmt.Println("Client ID : ", string(clientId))
			}

			req := ParseProduceRequest(p)

			if req.Acks == 0 {
				continue
			} else {
				// have to send response
				resp := new(bytes.Buffer)
				writeInt32(resp, int32(correlationId))
				writeInt32(resp, int32(len(req.Topics)))
				for _, topic := range req.Topics {
					writeInt16(resp, int16(len(topic.TopicName)))
					resp.Write([]byte(topic.TopicName))
					writeInt32(resp, int32(len(topic.Partitions)))
					for _, partition := range topic.Partitions {
						writeInt32(resp, partition.PartitionId)
						writeInt16(resp, 0)
						writeInt64(resp, 0)
					}
				}
				size := resp.Len()
				frameByte := new(bytes.Buffer)
				binary.Write(frameByte, binary.BigEndian, int32(size))
				must(conn.Write(frameByte.Bytes()))
				must(conn.Write(resp.Bytes()))

				continue
			}
		}
		res := make([]byte, 8)
		binary.BigEndian.PutUint32(res[:4], 4)
		binary.BigEndian.PutUint32(res[4:], correlationId)
		must(conn.Write(res))
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
