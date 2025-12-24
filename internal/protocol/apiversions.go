package protocol

import (
	"bytes"
	"encoding/binary"
)

type ApiKey struct {
	Apikey     int
	MinVersion int
	MaxVersion int
}

func handleApiVersionsRequest(header RequestHeader) ([]byte, error) {
	//this is not related to the apiVersion part of the request header
	//it is a type of request defined in kafka where apiKey = 18
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32(header.CorrelationId))
	binary.Write(buf, binary.BigEndian, int16(0))

	apiKeys := []ApiKey{
		{Apikey: 18, MinVersion: 0, MaxVersion: 4},
	}
	apiKeyLen := len(apiKeys)
	binary.Write(buf, binary.BigEndian, int32(apiKeyLen))
	for i := 0; i < apiKeyLen; i++ {
		binary.Write(buf, binary.BigEndian, int16(apiKeys[i].Apikey))
		binary.Write(buf, binary.BigEndian, int16(apiKeys[i].MinVersion))
		binary.Write(buf, binary.BigEndian, int16(apiKeys[i].MaxVersion))
	}
	return buf.Bytes(), nil
}
