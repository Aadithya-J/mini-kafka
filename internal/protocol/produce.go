package protocol

import (
	"bytes"
	"fmt"

	storage "github.com/Aadithya-J/mini-kafka/internal/storage"
)

const (
	maxTransactionalIdLen = 1024
	maxTopics             = 1000
	maxTopicNameLen       = 255
	maxPartitionsPerTopic = 1000
	maxMessageSetSize     = 10 * 1024 * 1024
)

func handleProduceRequest(header RequestHeader, msg *bytes.Reader) ([]byte, error) {
	req, err := parseProduceRequest(msg)
	if err != nil {
		return nil, fmt.Errorf("parse produce request: %w", err)
	}

	// If acks == 0 no resp
	if req.Acks == 0 {
		return nil, nil
	}

	resp := new(bytes.Buffer)
	writeInt32(resp, int32(header.CorrelationId))
	writeInt32(resp, int32(len(req.Topics)))
	for _, topic := range req.Topics {
		writeInt16(resp, int16(len(topic.TopicName)))
		resp.Write([]byte(topic.TopicName))
		writeInt32(resp, int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
			// TODO: maybe asynchronous not sure
			err := storage.SaveData(topic.TopicName, partition.PartitionId, partition.MessageSet)
			writeInt32(resp, partition.PartitionId)
			if err != nil {
				writeInt16(resp, 10)
			} else {
				writeInt16(resp, 0)
			}
			writeInt64(resp, 0)
		}
	}
	return resp.Bytes(), nil
}

func parseProduceRequest(p *bytes.Reader) (ProduceRequest, error) {
	var req ProduceRequest
	var err error

	transactionalIdLen, err := readInt16(p)
	if err != nil {
		return req, fmt.Errorf("read transactionalIdLen: %w", err)
	}
	if transactionalIdLen != -1 {
		if transactionalIdLen < 0 || transactionalIdLen > maxTransactionalIdLen {
			return req, fmt.Errorf("transactionalIdLen out of range: %d", transactionalIdLen)
		}
		transactionId, err := readBytes(p, int32(transactionalIdLen))
		if err != nil {
			return req, fmt.Errorf("read transactionalId: %w", err)
		}
		s := string(transactionId)
		req.TransactionalId = &s
	}

	if req.Acks, err = readInt16(p); err != nil {
		return req, fmt.Errorf("read acks: %w", err)
	}
	if req.Timeout, err = readInt32(p); err != nil {
		return req, fmt.Errorf("read timeout: %w", err)
	}

	topicArrayLen, err := readInt32(p)
	if err != nil {
		return req, fmt.Errorf("read topics array length: %w", err)
	}
	if topicArrayLen < 0 || topicArrayLen > maxTopics {
		return req, fmt.Errorf("topic array length out of range: %d", topicArrayLen)
	}

	req.Topics = make([]TopicData, topicArrayLen)
	for i := 0; i < int(topicArrayLen); i++ {
		topicNameLen, err := readInt16(p)
		if err != nil {
			return req, fmt.Errorf("topic[%d] name length: %w", i, err)
		}
		if topicNameLen < 0 || topicNameLen > maxTopicNameLen {
			return req, fmt.Errorf("topic[%d] name length out of range: %d", i, topicNameLen)
		}
		topicname, err := readBytes(p, int32(topicNameLen))
		if err != nil {
			return req, fmt.Errorf("topic[%d] name bytes: %w", i, err)
		}
		req.Topics[i].TopicName = string(topicname)

		partitionsCount, err := readInt32(p)
		if err != nil {
			return req, fmt.Errorf("topic[%d] partitions count: %w", i, err)
		}
		if partitionsCount < 0 || partitionsCount > maxPartitionsPerTopic {
			return req, fmt.Errorf("topic[%d] partitions count out of range: %d", i, partitionsCount)
		}
		req.Topics[i].Partitions = make([]PartitionData, partitionsCount)

		for j := 0; j < int(partitionsCount); j++ {
			if req.Topics[i].Partitions[j].PartitionId, err = readInt32(p); err != nil {
				return req, fmt.Errorf("topic[%d] partition[%d] id: %w", i, j, err)
			}
			messageSetSize, err := readInt32(p)
			if err != nil {
				return req, fmt.Errorf("topic[%d] partition[%d] messageSetSize: %w", i, j, err)
			}
			if messageSetSize < 0 || messageSetSize > maxMessageSetSize {
				return req, fmt.Errorf("topic[%d] partition[%d] messageSetSize out of range: %d", i, j, messageSetSize)
			}
			// TODO: Have to parse message set i think
			req.Topics[i].Partitions[j].MessageSet, err = readBytes(p, messageSetSize)
			if err != nil {
				return req, fmt.Errorf("topic[%d] partition[%d] messageSet read: %w", i, j, err)
			}
		}
	}
	return req, nil
}
