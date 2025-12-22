package main

import (
	"bytes"
	"fmt"
)

func ParseProduceRequest(p *bytes.Reader) ProduceRequest {
	req := ProduceRequest{}
	transactionalIdLen := must(readInt16(p))
	if transactionalIdLen != -1 {
		s := string(must(readBytes(p, int32(transactionalIdLen))))
		req.TransactionalId = &s
		fmt.Println("TransactionalId:", s)
	}
	req.Acks = must(readInt16(p))
	req.Timeout = must(readInt32(p))
	TopicArrayLength := must(readInt32(p))
	req.Topics = make([]TopicData, TopicArrayLength)
	for i := 0; i < int(TopicArrayLength); i++ {
		topicNameLen := must(readInt16(p))
		req.Topics[i].TopicName = string(must(readBytes(p, int32(topicNameLen))))
		fmt.Println("Topic Name:", req.Topics[i].TopicName)
		M := must(readInt32(p))
		req.Topics[i].Partitions = make([]PartitionData, M)
		for j := 0; j < int(M); j++ {
			req.Topics[i].Partitions[j].PartitionId = must(readInt32(p))
			messageSetSize := must(readInt32(p))
			req.Topics[i].Partitions[j].MessageSet = must(readBytes(p, messageSetSize))
			//not parsing message set for now
			fmt.Println(
				"Partition ID, MessageSetSize, MessageSetLen:",
				req.Topics[i].Partitions[j].PartitionId,
				messageSetSize,
				len(req.Topics[i].Partitions[j].MessageSet),
			)
		}
	}
	return req
}
