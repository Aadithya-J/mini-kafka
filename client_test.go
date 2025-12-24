package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

const serverAddr = "localhost:8080"

func writeInt16(w *bytes.Buffer, n int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(n))
	w.Write(b[:])
}

func writeInt32(w *bytes.Buffer, n int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(n))
	w.Write(b[:])
}

func writeInt64(w *bytes.Buffer, n int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}

// TestApiVersionsRequest tests the ApiVersions request (ApiKey: 18)
func TestApiVersionsRequest(t *testing.T) {
	conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server at %s: %v (Is the server running?)", serverAddr, err)
	}
	defer conn.Close()

	req := buildApiVersionsRequest()
	_, err = conn.Write(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Read response size (4 bytes)
	sizeBuf := make([]byte, 4)
	_, err = conn.Read(sizeBuf)
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)
	if size == 0 {
		t.Fatal("Received 0-sized response")
	}

	// Read response body
	responseBuf := make([]byte, size)
	_, err = conn.Read(responseBuf)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	t.Logf("ApiVersions Response received, size: %d bytes", size)
}

// TestProduceRequest tests the Produce request (ApiKey: 0) with Acks=1
func TestProduceRequest(t *testing.T) {
	conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server at %s: %v", serverAddr, err)
	}
	defer conn.Close()

	req := buildProduceRequest("test-topic", 0, "Hola, Kafka!", 1)
	_, err = conn.Write(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	sizeBuf := make([]byte, 4)
	_, err = conn.Read(sizeBuf)
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)
	responseBuf := make([]byte, size)
	_, err = conn.Read(responseBuf)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	t.Logf("Produce Response received, size: %d bytes", size)
}

// TestProduceRequestNoAck tests fire-and-forget (Acks=0)
func TestProduceRequestNoAck(t *testing.T) {
	conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	req := buildProduceRequest("test-topic", 0, "Fire and forget message", 0)
	_, err = conn.Write(req)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Server should not send a response for Acks=0
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Error("Expected no response for Acks=0, but received something")
	}

	t.Log("Fire and forget message sent successfully")
}

// buildApiVersionsRequest builds an ApiVersions request (ApiKey: 18)
func buildApiVersionsRequest() []byte {
	body := new(bytes.Buffer)

	apiKey := int16(18)
	apiVersion := int16(0)
	correlationId := int32(1)
	clientIdLen := int16(11)
	clientId := "test-client"

	writeInt16(body, apiKey)
	writeInt16(body, apiVersion)
	writeInt32(body, correlationId)
	writeInt16(body, clientIdLen)
	body.WriteString(clientId)

	frame := new(bytes.Buffer)
	writeInt32(frame, int32(body.Len()))
	frame.Write(body.Bytes())

	return frame.Bytes()
}

// buildProduceRequest builds a Produce request (ApiKey: 0)
func buildProduceRequest(topic string, partition int32, message string, acks int16) []byte {
	body := new(bytes.Buffer)

	apiKey := int16(0)
	apiVersion := int16(2)
	correlationId := int32(2)
	clientIdLen := int16(11)
	clientId := "test-client"

	// Header
	writeInt16(body, apiKey)
	writeInt16(body, apiVersion)
	writeInt32(body, correlationId)
	writeInt16(body, clientIdLen)
	body.WriteString(clientId)

	// Produce Request Body
	writeInt16(body, int16(-1)) // transactional_id (null)
	writeInt16(body, acks)
	writeInt32(body, 1000) // timeout

	// Topics
	writeInt32(body, 1) // 1 topic
	writeInt16(body, int16(len(topic)))
	body.WriteString(topic)

	// Partitions
	writeInt32(body, 1) // 1 partition
	writeInt32(body, partition)

	// Message Set
	msgBytes := []byte(message)
	writeInt32(body, int32(len(msgBytes)))
	body.Write(msgBytes)

	// Frame
	frame := new(bytes.Buffer)
	writeInt32(frame, int32(body.Len()))
	frame.Write(body.Bytes())

	return frame.Bytes()
}

type ClientIntegrationTest struct {
	addr string
}

func NewClientIntegrationTest(addr string) *ClientIntegrationTest {
	return &ClientIntegrationTest{addr: addr}
}

func (c *ClientIntegrationTest) SendMessage(topic string, partition int32, message string, acks int16) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	req := buildProduceRequest(topic, partition, message, acks)
	_, err = conn.Write(req)
	if err != nil {
		return nil, err
	}

	if acks == 0 {
		return nil, nil
	}

	sizeBuf := make([]byte, 4)
	_, err = conn.Read(sizeBuf)
	if err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(sizeBuf)
	resp := make([]byte, size)
	_, err = conn.Read(resp)
	return resp, err
}

func ExampleClientIntegrationTest_SendMessage() {
	client := NewClientIntegrationTest("localhost:8080")
	resp, err := client.SendMessage("my-topic", 0, "hello", 1)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Received response of size %d\n", len(resp))
}
