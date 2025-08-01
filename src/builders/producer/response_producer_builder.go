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
	name             string
	bootstrapServers string
	acks             producer_types.Acks
	replyTopic       string
	// --- BLOQUE AÑADIDO ---
	// Hacemos que el timeout de conexión sea configurable.
	ConnectionTimeout int
	// --- FIN DEL BLOQUE AÑADIDO ---
}

// NewResponseProducerBuilder crea un nuevo ResponseProducerBuilder
func NewResponseProducerBuilder(bootstrapServers string, name string) *ResponseProducerBuilder {
	return &ResponseProducerBuilder{
		name:             name,
		bootstrapServers: bootstrapServers,
		acks:             producer_types.AcksAll,
		// --- LÍNEA AÑADIDA ---
		ConnectionTimeout: 10000, // 10 segundos por defecto
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

// --- BLOQUE AÑADIDO ---
// SetConnectionTimeout permite al usuario configurar el tiempo de espera.
func (b *ResponseProducerBuilder) SetConnectionTimeout(timeoutMs int) *ResponseProducerBuilder {
	if timeoutMs > 0 {
		b.ConnectionTimeout = timeoutMs
	}
	return b
}

// --- FIN DEL BLOQUE AÑADIDO ---

// Build construye y retorna un ResponseProducer
func (b *ResponseProducerBuilder) Build() (types.ResponseProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.bootstrapServers,
		"client.id":         b.name,
		"acks":              string(b.acks),
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

	// --- BLOQUE AÑADIDO: LA SOLUCIÓN FINAL ---
	// Antes de devolver el productor, nos aseguramos de que esté completamente conectado.
	if err := responseProducer.WaitForConnection(b.ConnectionTimeout); err != nil {
		responseProducer.Close() // Limpiamos los recursos si la conexión falla.
		return nil, fmt.Errorf("no se pudo establecer conexión para ResponseProducer: %w", err)
	}
	// --- FIN DEL BLOQUE AÑADIDO ---

	return responseProducer, nil
}
