package producer

import (
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaProducer implementa la interfaz Producer usando confluent-kafka-go
type SingleProducer struct {
	name          string
	kafkaProducer *kafka.Producer
	options       []types.Option
}

func NewSingleProducer(name string, kafkaProducer *kafka.Producer, options []types.Option) *SingleProducer {
	return &SingleProducer{
		name:          name,
		kafkaProducer: kafkaProducer,
		options:       options,
	}
}
func (p *SingleProducer) Name() string {
	return p.name
}

func (p *SingleProducer) Produce(topic string, key string, value []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}
	return p.kafkaProducer.Produce(msg, nil)
}

// ProduceWithHeaders envía un mensaje a Kafka con encabezados personalizados
func (p *SingleProducer) ProduceWithHeaders(topic string, key []byte, value []byte, headers []types.Header) error {
	// Convertir los encabezados personalizados al formato de Kafka
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for _, header := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   header.Key(),
			Value: []byte(header.Value()),
		})
	}

	// Crear el mensaje con encabezados
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        kafkaHeaders,
	}

	// Enviar el mensaje
	return p.kafkaProducer.Produce(msg, nil)
}

func (p *SingleProducer) Flush(timeoutMs int) int {
	return p.kafkaProducer.Flush(timeoutMs)
}

func (p *SingleProducer) Close() error {
	p.kafkaProducer.Close()
	return nil
}

func (p *SingleProducer) Options() []types.Option {
	return p.options
}

// GetKafkaProducer devuelve el productor Kafka interno
func (p *SingleProducer) GetKafkaProducer() *kafka.Producer {
	return p.kafkaProducer
}
