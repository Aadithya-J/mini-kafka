package protocol

import (
	"bytes"
	"log"
	storage "github.com/Aadithya-J/mini-kafka/internal/storage"
)
// put in request header
			// clientIdLen := must(readInt16(msg))
			// if clientIdLen != -1 {
			// 	clientId := must(readBytes(msg, int32(clientIdLen)))
			// 	fmt.Println("Client ID : ", string(clientId))
			// }

func handleProduceRequest(header RequestHeader, msg *bytes.Reader) ([]byte, error) {

	req := parseProduceRequest(msg)
	if req.Acks == 0{
		return nil,nil
	} else {

	}
	// have to send response
	resp := new(bytes.Buffer)
	writeInt32(resp, int32(header.CorrelationId))
	writeInt32(resp, int32(len(req.Topics)))
	for _, topic := range req.Topics {
		writeInt16(resp, int16(len(topic.TopicName)))
		resp.Write([]byte(topic.TopicName))
		writeInt32(resp, int32(len(topic.Partitions)))
		for _, partition := range topic.Partitions {
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
	return resp.Bytes(),nil
}

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

