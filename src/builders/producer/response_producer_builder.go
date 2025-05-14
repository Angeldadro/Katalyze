package producer_builder

import (
	producer_types "github.com/Angeldadro/Katalyze/src/builders/producer/types"
	"github.com/Angeldadro/Katalyze/src/producer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResponseProducerBuilder es el builder para ResponseProducer
type ResponseProducerBuilder struct {
	name             string
	bootstrapServers string
	acks             producer_types.Acks
	replyTopic       string
}

// NewResponseProducerBuilder crea un nuevo ResponseProducerBuilder
func NewResponseProducerBuilder(bootstrapServers string, name string) *ResponseProducerBuilder {
	return &ResponseProducerBuilder{
		name:             name,
		bootstrapServers: bootstrapServers,
		acks:             producer_types.AcksAll,
	}
}

// SetAcks establece el nivel de confirmación para el productor
func (b *ResponseProducerBuilder) SetAcks(acks producer_types.Acks) *ResponseProducerBuilder {
	b.acks = acks
	return b
}

// SetReplyTopic establece el topic para las respuestas
func (b *ResponseProducerBuilder) SetReplyTopic(replyTopic string) *ResponseProducerBuilder {
	b.replyTopic = replyTopic
	return b
}

// Build construye y retorna un ResponseProducer
func (b *ResponseProducerBuilder) Build() (types.ResponseProducer, error) {
	// Crear configuración básica para el productor
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.bootstrapServers,
		"client.id":         b.name,
		"acks":              string(b.acks),
	}

	// Crear productor de Kafka
	kafkaProducer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	// Determinar el topic de respuesta si no se ha especificado
	replyTopic := b.replyTopic
	if replyTopic == "" {
		replyTopic = b.name + "-reply"
	}

	// Convertir ConfigMap a Options
	options := utils.MapConfigMapToOptions(config)

	// Crear y retornar ResponseProducer
	return producer.NewResponseProducer(
		b.name,
		b.bootstrapServers,
		kafkaProducer,
		replyTopic,
		options,
	)
}
