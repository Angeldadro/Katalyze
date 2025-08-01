package producer_builder

import (
	"fmt"

	producer_types "github.com/Angeldadro/Katalyze/src/builders/producer/types"
	"github.com/Angeldadro/Katalyze/src/producer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResponseProducerBuilder es el builder para ResponseProducer
type ResponseProducerBuilder struct {
	name              string
	bootstrapServers  string
	acks              producer_types.Acks
	replyTopic        string
	ConnectionTimeout int
	config            map[string]interface{} // Campo para configuración personalizada
}

// NewResponseProducerBuilder crea un nuevo ResponseProducerBuilder
func NewResponseProducerBuilder(bootstrapServers string, name string) *ResponseProducerBuilder {
	return &ResponseProducerBuilder{
		name:              name,
		bootstrapServers:  bootstrapServers,
		acks:              producer_types.AcksAll,
		ConnectionTimeout: 10000, // 10 segundos por defecto
		config:            make(map[string]interface{}),
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

// SetConnectionTimeout permite al usuario configurar el tiempo de espera.
func (b *ResponseProducerBuilder) SetConnectionTimeout(timeoutMs int) *ResponseProducerBuilder {
	if timeoutMs > 0 {
		b.ConnectionTimeout = timeoutMs
	}
	return b
}

// --- MÉTODO AÑADIDO (DE VUELTA) ---
// SetConfig permite añadir una configuración personalizada al productor.
func (b *ResponseProducerBuilder) SetConfig(key string, value interface{}) *ResponseProducerBuilder {
	b.config[key] = value
	return b
}

// --- FIN DEL MÉTODO AÑADIDO ---

// Build construye y retorna un ResponseProducer
func (b *ResponseProducerBuilder) Build() (types.ResponseProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.bootstrapServers,
		"client.id":         b.name,
		"acks":              string(b.acks),
	}

	// Aplicar la configuración personalizada
	for key, value := range b.config {
		(*config)[key] = value
	}

	kafkaProducer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	replyTopic := b.replyTopic
	if replyTopic == "" {
		replyTopic = b.name + "-reply"
	}

	options := utils.MapConfigMapToOptions(config)

	responseProducer, err := producer.NewResponseProducer(
		b.name,
		b.bootstrapServers,
		kafkaProducer,
		replyTopic,
		options,
	)
	if err != nil {
		kafkaProducer.Close()
		return nil, err
	}

	if err := responseProducer.WaitForConnection(b.ConnectionTimeout); err != nil {
		responseProducer.Close()
		return nil, fmt.Errorf("no se pudo establecer conexión para ResponseProducer: %w", err)
	}

	return responseProducer, nil
}
