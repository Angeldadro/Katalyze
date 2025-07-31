// Archivo: Katalyze/src/builders/producer/response_producer_builder.go
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
	config           map[string]interface{} // <-- AÑADIDO: Campo para configuración personalizada
}

// NewResponseProducerBuilder crea un nuevo ResponseProducerBuilder
func NewResponseProducerBuilder(bootstrapServers string, name string) *ResponseProducerBuilder {
	return &ResponseProducerBuilder{
		name:             name,
		bootstrapServers: bootstrapServers,
		acks:             producer_types.AcksAll,
		config:           make(map[string]interface{}), // <-- AÑADIDO: Inicializar el mapa
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

// SetConfig permite añadir una configuración personalizada al productor. <-- AÑADIDO
func (b *ResponseProducerBuilder) SetConfig(key string, value interface{}) *ResponseProducerBuilder {
	b.config[key] = value
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

	// --- LÍNEAS AÑADIDAS ---
	// Aplicar la configuración personalizada
	for key, value := range b.config {
		(*config)[key] = value
	}
	// --- FIN DE LÍNEAS AÑADIDAS ---

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
