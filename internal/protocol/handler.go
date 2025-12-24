package protocol

import (
	"bytes"
	"fmt"
)

type ApiKey int16

const (
	ApiKeyProduce     ApiKey = 0
	ApiKeyApiVersions ApiKey = 18
)


func ProcessFrame(msg []byte) ([]byte, error) {
	r := bytes.NewReader(msg)
	reqHeader, err := parseRequestHeader(r)
	if err != nil {
		return nil, fmt.Errorf("parse request header: %w", err)
	}

	switch reqHeader.ApiKey {
	case ApiKeyApiVersions:
		resp, err := handleApiVersionsRequest(reqHeader)
		if err != nil {
			return buildProduceErrorResponse(int32(reqHeader.CorrelationId), err), nil
		}
		return resp, nil

	case ApiKeyProduce:
		resp, err := handleProduceRequest(reqHeader, r)
		if err != nil {
			return buildProduceErrorResponse(int32(reqHeader.CorrelationId), err), nil
		}
		return resp, nil

	default:
		return buildProduceErrorResponse(int32(reqHeader.CorrelationId), fmt.Errorf("unsupported api key %d", reqHeader.ApiKey)), nil
	}
}


func buildProduceErrorResponse(correlationId int32, cause error) []byte {
	resp := new(bytes.Buffer)
	writeInt32(resp, correlationId)
	// topics array length 0
	writeInt32(resp, 0)
	return resp.Bytes()
}
