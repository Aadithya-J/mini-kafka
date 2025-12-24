package protocol

import (
	"bytes"
	"log"
)

func parseProduceRequest(p *bytes.Reader) ProduceRequest {
	req := ProduceRequest{}
	transactionalIdLen,err := readInt16(p)
	if err != nil {
		panic(err)
	}
	if transactionalIdLen != -1 {
		transactionId,err := readBytes(p,int32(transactionalIdLen))
		if err != nil {
			panic(err)
		}
		s := string(transactionId)
		req.TransactionalId = &s
		log.Println("TransactionalId:", s)
	}
	req.Acks,err = readInt16(p)
	req.Timeout,err = readInt32(p)
	TopicArrayLength,err :=readInt32(p)
	req.Topics = make([]TopicData, TopicArrayLength)
	for i := 0; i < int(TopicArrayLength); i++ {
		topicNameLen,err := readInt16(p)
		if err != nil {
			panic(err)
		}
		topicname,err := readBytes(p,int32(topicNameLen))
		req.Topics[i].TopicName = string(topicname)
		// log.Println("Topic Name:", req.Topics[i].TopicName)
		M,err := readInt32(p)
		req.Topics[i].Partitions = make([]PartitionData, M)
		for j := 0; j < int(M); j++ {
			req.Topics[i].Partitions[j].PartitionId,err = readInt32(p)
			messageSetSize,err := readInt32(p)
			if err != nil {
				panic(err)
			}
			req.Topics[i].Partitions[j].MessageSet,err = readBytes(p, messageSetSize)
			//not parsing message set for now
			log.Println(
				"Partition ID, MessageSetSize, MessageSetLen:",
				req.Topics[i].Partitions[j].PartitionId,
				messageSetSize,
				len(req.Topics[i].Partitions[j].MessageSet),
			)
		}
	}
	return req
}
