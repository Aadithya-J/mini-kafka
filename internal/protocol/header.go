package protocol

import (
	"bytes"
	"fmt"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      []byte
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

	clientIdLen, err := readInt16(r)
	if err != nil {
		return RequestHeader{}, fmt.Errorf("read clientIdlen: %w", err)
	}
	var clientId []byte
	if clientIdLen > 0 {
		clientId, err = readBytes(r, int32(clientIdLen))
		if err != nil {
			return RequestHeader{}, fmt.Errorf("read ClientId: %w", err)
		}
	} else {

	}
	// if clientIdLen != -1 {
	// 	clientId := must(readBytes(msg, int32(clientIdLen)))
	// 	fmt.Println("Client ID : ", string(clientId))
	// }

	return RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationId: correlationId,
		ClientId:      clientId,
	}, nil
}
