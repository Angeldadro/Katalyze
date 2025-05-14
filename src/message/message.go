package message

import (
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	msg *kafka.Message
}

func NewMessage(msg *kafka.Message) *Message {
	return &Message{msg: msg}
}

func (m *Message) Key() []byte   { return m.msg.Key }
func (m *Message) Value() []byte { return m.msg.Value }
func (m *Message) Topic() string { return *m.msg.TopicPartition.Topic }
func (m *Message) Headers() []types.Header {
	origHeaders := m.msg.Headers
	typesHeaders := make([]types.Header, len(origHeaders))
	for i, h := range origHeaders {
		typesHeaders[i] = &Header{h}
	}
	return typesHeaders
}
func (m *Message) Partition() int { return int(m.msg.TopicPartition.Partition) }
func (m *Message) Offset() int64  { return int64(m.msg.TopicPartition.Offset) }
