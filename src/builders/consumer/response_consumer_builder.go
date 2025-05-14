package consumer_builder

import (
	"fmt"

	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/consumer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResponseConsumerBuilder es el builder para ResponseConsumer
type ResponseConsumerBuilder struct {
	bootstrapServers string
	groupID          string
	topics           []string
	AutoOffsetReset  consumer_types.AutoOffsetReset
	EnableAutoCommit *bool
	responseProducer types.SingleProducer
}

// NewResponseConsumerBuilder crea un nuevo ResponseConsumerBuilder
func NewResponseConsumerBuilder(bootstrapServers string, topics []string) *ResponseConsumerBuilder {
	return &ResponseConsumerBuilder{
		bootstrapServers: bootstrapServers,
		topics:           topics,
	}
}

// SetGroupID establece el ID del grupo de consumidores
func (b *ResponseConsumerBuilder) SetGroupID(groupID string) *ResponseConsumerBuilder {
	b.groupID = groupID
	return b
}

// SetAutoOffsetReset establece desde dónde comenzar a consumir mensajes (earliest o latest)
func (b *ResponseConsumerBuilder) SetAutoOffsetReset(autoOffsetReset consumer_types.AutoOffsetReset) *ResponseConsumerBuilder {
	b.AutoOffsetReset = autoOffsetReset
	return b
}

// SetEnableAutoCommit establece si se debe hacer commit automático de los offsets
func (b *ResponseConsumerBuilder) SetEnableAutoCommit(enableAutoCommit bool) *ResponseConsumerBuilder {
	b.EnableAutoCommit = &enableAutoCommit
	return b
}

// SetResponseProducer establece el producer de respuestas
func (b *ResponseConsumerBuilder) SetResponseProducer(responseProducer types.SingleProducer) *ResponseConsumerBuilder {
	b.responseProducer = responseProducer
	return b
}

// Build construye y retorna un ResponseConsumer
func (b *ResponseConsumerBuilder) Build() (types.ResponseConsumer, error) {
	// Validar parámetros requeridos
	if b.bootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap.servers es requerido")
	}
	if len(b.topics) == 0 {
		return nil, fmt.Errorf("un topic es requerido")
	}

	// Crear configuración básica
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.bootstrapServers,
	}

	if b.groupID != "" {
		(*config)["group.id"] = b.groupID
	} else {
		(*config)["group.id"] = b.topics[0]
	}
	// Configurar auto.offset.reset
	if b.AutoOffsetReset != "" {
		(*config)["auto.offset.reset"] = string(b.AutoOffsetReset)
	} else {
		(*config)["auto.offset.reset"] = string(consumer_types.AutoOffsetResetLatest)
	}
	// Configurar enable.auto.commit
	if b.EnableAutoCommit != nil {
		(*config)["enable.auto.commit"] = *b.EnableAutoCommit
	} else {
		(*config)["enable.auto.commit"] = true
	}

	if b.responseProducer == nil {
		return nil, fmt.Errorf("response.producer es requerido")
	}
	// Crear el consumidor de Kafka
	kafkaConsumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	// Convertir ConfigMap a Options
	options := utils.MapConfigMapToOptions(config)

	// Crear y retornar el ResponseConsumer
	responseConsumer := consumer.NewResponseConsumer(kafkaConsumer, b.responseProducer, b.bootstrapServers, options, b.topics, b.groupID)
	return responseConsumer, nil
}
