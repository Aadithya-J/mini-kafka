package protocol

import (
	"bytes"
	"fmt"
)

func ProcessFrame(msg []byte) ([]byte, error) {
	r := bytes.NewReader(msg)
	reqHeader, err := parseRequestHeader(r)
	if err != nil {
		return nil, fmt.Errorf("Error parsing request header: %w", err)
	}
	switch reqHeader.ApiKey {
	case 18:
		return handleApiVersionsRequest(reqHeader)
	case 0:
		return handleProduceRequest(reqHeader, r)
	default:
		return nil, fmt.Errorf("Unsupported Api Key")
	}
}
