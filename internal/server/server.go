package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"github.com/Aadithya-J/mini-kafka/internal/protocol"
)

type Server struct {
	mu      sync.Mutex
	running bool
	lis     net.Listener
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) Start(addr string) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("server already running")
	}
	s.running = true
	s.mu.Unlock()

	lis, err := net.Listen("tcp",addr)

	if err != nil{
		return err
	}
	s.lis = lis
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(s.ctx,conn)
	}
}

func (s* Server) handleConn(ctx context.Context, conn net.Conn){
	defer s.wg.Done()
	defer conn.Close()


	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	for {

		err := handleRequest(connCtx,conn)


	}

	

}

func handleRequest(ctx context.Context, conn net.Conn) error {
	reqHeader,err := readRequestHeader(connCtx,conn)
	if err != nil {
		return
	}
	switch reqHeader.ApiKey{
	case 18:
		handleApiVersionsRequest(reqHeader,conn)
	case 0:
		handleProduceRequest(reqHeader,conn)
	default:
		return errors.ErrUnsupported
	}
}

func readRequestHeader(ctx context.Context, conn net.Conn) (RequestHeader,error) {
	msgBytes,err := readFrame(conn)
	if err != nil {
		return RequestHeader{},err
	}
	msg := bytes.NewReader(msgBytes)
	reqHeader,err := parseRequestHeader(msg)
	return reqHeader,nil
}

type RequestHeader struct {
	ApiKey int16
	ApiVersion int16
	CorrelationId int32
}

func parseRequestHeader(r *bytes.Reader) (RequestHeader, error) {
	apiKey, err := readInt16(r)
	if err != nil {
		return RequestHeader{}, fmt.Errorf("read apiKey: %w", err)
	}

	apiVersion, err := readInt16(r)
	if err != nil {
		return RequestHeader{}, fmt.Errorf("read apiVersion: %w", err)
	}

	correlationId, err := readInt32(r)
	if err != nil {
		return RequestHeader{}, fmt.Errorf("read correlationId: %w", err)
	}

	return RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationId: correlationId,
	}, nil
}



func handleConn(conn net.Conn) {
	for {

		apiKey := must(readInt16(msg))
		apiVersion := must(readInt16(msg))
		correlationId := must(readInt32(msg))
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

			clientIdLen := must(readInt16(msg))
			if clientIdLen != -1 {
				clientId := must(readBytes(msg, int32(clientIdLen)))
				fmt.Println("Client ID : ", string(clientId))
			}

			req := ParseProduceRequest(msg)

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
						err = saveData(topic.TopicName, partition.PartitionId, partition.MessageSet)
						writeInt32(resp, partition.PartitionId)
						if err != nil {
							writeInt16(resp, 10)
						} else {
							writeInt16(resp, 0)
						}
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
		binary.BigEndian.PutUint32(res[4:], uint32(correlationId))
		must(conn.Write(res))
	}
}



func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return errors.New("server not running")
	}
	s.running = false
	s.mu.Unlock()
	s.cancel()
	if s.lis != nil {
		s.lis.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
