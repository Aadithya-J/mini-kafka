package protocol

type ProduceRequest struct {
	TransactionalId *string
	Acks            int16
	Timeout         int32
	Topics          []TopicData
}

type TopicData struct {
	TopicName  string
	Partitions []PartitionData
}

type PartitionData struct {
	PartitionId int32
	MessageSet  []byte
}
