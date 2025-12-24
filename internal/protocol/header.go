package protocol

import (
	"bytes"
	"fmt"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
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
