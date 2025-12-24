package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
)

type RequestHeader struct {
	ApiKey int16
	ApiVersion int16
	CorrelationId int32
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
	//will need in future maybe

	for {
		msg,err := readFrame(conn)
		if err != nil {
			return
		}
		resp,err := processFrame(msg)
		if err != nil {
			return
		}
		_,err = conn.Write(resp)
		if err != nil {
			return 
		}
	}
}

func processFrame(msg []byte) ([]byte,error) {
	r := bytes.NewReader(msg)	
	reqHeader,err := parseRequestHeader(r)
	if err != nil {
		return nil,fmt.Errorf("Error parsing request header: %v",err)
	}
	switch reqHeader.ApiKey{
	case 18:
		return handleApiVersionsRequest(reqHeader)
	case 0:
		return handleProduceRequest(reqHeader,r)
	default:
		return nil,fmt.Errorf("Unsupported Api Key")
	}
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

func handleApiVersionsRequest(RequestHeader) ([]byte,error){
	var q []byte
	return q,nil
}

func handleProduceRequest(RequestHeader,*bytes.Reader) ([]byte,error){
	var q []byte
	return q,nil
}