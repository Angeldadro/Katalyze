package message

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// KafkaHeader es un adaptador para kafka.Header que implementa types.Header
type Header struct {
	header kafka.Header
}

func NewHeader(header kafka.Header) *Header {
	return &Header{header: header}
}

func NewHeaderFromKV(key string, value string) *Header {
	return &Header{header: kafka.Header{Key: key, Value: []byte(value)}}
}

func (h *Header) Key() string   { return h.header.Key }
func (h *Header) Value() string { return string(h.header.Value) }
