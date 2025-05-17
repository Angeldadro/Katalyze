package producer

import (
	"fmt"
	"time"

	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaProducer implementa la interfaz Producer usando confluent-kafka-go
type SingleProducer struct {
	name          string
	kafkaProducer *kafka.Producer
	options       []types.Option
	connected     bool
}

func NewSingleProducer(name string, kafkaProducer *kafka.Producer, options []types.Option) *SingleProducer {
	return &SingleProducer{
		name:          name,
		kafkaProducer: kafkaProducer,
		options:       options,
		connected:     false,
	}
}
func (p *SingleProducer) Name() string {
	return p.name
}

func (p *SingleProducer) Produce(topic string, key string, value []byte) error {
	if err := p.ensureConnected(); err != nil {
		return err
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}
	return p.kafkaProducer.Produce(msg, nil)
}

// ProduceWithHeaders envía un mensaje a Kafka con encabezados personalizados
func (p *SingleProducer) ProduceWithHeaders(topic string, key []byte, value []byte, headers []types.Header) error {
	if err := p.ensureConnected(); err != nil {
		return err
	}

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

// WaitForConnection espera a que la conexión a Kafka esté establecida
// timeout es el tiempo máximo de espera en milisegundos
// retryInterval es el intervalo entre intentos en milisegundos
func (p *SingleProducer) WaitForConnection(timeout, retryInterval int) error {
	start := time.Now()
	timeoutDuration := time.Duration(timeout) * time.Millisecond
	retryDuration := time.Duration(retryInterval) * time.Millisecond

	for !p.connected {
		if time.Since(start) > timeoutDuration {
			return fmt.Errorf("timeout esperando conexión a Kafka")
		}

		// Intentar verificar la conexión
		if p.checkConnection() {
			p.connected = true
			return nil
		}

		// Esperar antes del siguiente intento
		time.Sleep(retryDuration)
	}

	return nil
}

// checkConnection verifica si el productor puede conectarse a Kafka
func (p *SingleProducer) checkConnection() bool {
	// Usar Flush con un timeout pequeño para verificar la conexión
	// Si Flush devuelve 0, significa que no hay mensajes pendientes y la conexión está establecida
	// Si devuelve > 0, significa que hay mensajes pendientes y no se pudo conectar
	pendingMessages := p.kafkaProducer.Flush(1000)
	return pendingMessages == 0
}

// ensureConnected asegura que el productor esté conectado antes de producir mensajes
func (p *SingleProducer) ensureConnected() error {
	if p.connected {
		return nil
	}

	// Intentar conectar con un timeout razonable
	return p.WaitForConnection(10000, 500) // 10 segundos de timeout, 500ms entre intentos
}
