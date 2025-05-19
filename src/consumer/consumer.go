package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	DefaultConnectionTimeout = 30000
)

// KafkaConsumer implementa la interfaz SingleConsumer usando confluent-kafka-go
// y ReadMessage en vez de Poll.
type SingleConsumer struct {
	kafkaConsumer *kafka.Consumer
	options       []types.Option
	topics        []string
	groupID       string
	ctx           context.Context
	cancelCtx     context.CancelFunc
} // topic y groupID se asignarán al construir el consumer

func NewConsumer(kafkaConsumer *kafka.Consumer, options []types.Option, topics []string, groupID string) *SingleConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &SingleConsumer{
		kafkaConsumer: kafkaConsumer,
		options:       options,
		topics:        topics,
		groupID:       groupID,
		ctx:           ctx,
		cancelCtx:     cancel,
	}
}

func (c *SingleConsumer) Options() []types.Option {
	return c.options
}

func (c *SingleConsumer) SetKafkaConsumer(kafkaConsumer *kafka.Consumer) *SingleConsumer {
	c.kafkaConsumer = kafkaConsumer
	return c
}

// WaitForConnection espera a que el consumidor esté conectado a Kafka
// timeout es la duración máxima a esperar en milisegundos
// retorna error si no se puede conectar en el tiempo especificado
func (c *SingleConsumer) WaitForConnection(timeout int) error {
	if c.kafkaConsumer == nil {
		return errors.New("kafka consumer no inicializado")
	}

	// Primero intenta suscribirse a los tópicos
	err := c.kafkaConsumer.SubscribeTopics(c.topics, nil)
	if err != nil {
		return err
	}

	// Define un contexto con timeout para limitar el tiempo de espera
	timeoutDuration := time.Duration(timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	// Intervalo de verificación
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout esperando la conexión a Kafka")
		case <-tick.C:
			// Intenta obtener los metadatos para comprobar la conexión
			metadata, err := c.kafkaConsumer.GetMetadata(nil, true, int(timeoutDuration.Milliseconds()))
			if err == nil && len(metadata.Brokers) > 0 {
				// Conectado exitosamente
				return nil
			}
		}
	}
}

func (c *SingleConsumer) Subscribe(handler types.Handler) error {
	// Esperar a que esté conectado antes de iniciar el consumo
	err := c.WaitForConnection(DefaultConnectionTimeout)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			msg, err := c.ReadMessage(c.ctx, 1000)
			if err != nil {
				continue
			}
			// Procesando mensaje
			handler(msg)
		}
	}()
	return nil
}

func (c *SingleConsumer) ReadMessage(ctx context.Context, timeoutMs int) (*message.Message, error) {
	msg, err := c.kafkaConsumer.ReadMessage(time.Duration(timeoutMs) * time.Millisecond)
	if err != nil {
		return nil, err
	}
	return message.NewMessage(msg), nil
}

func (c *SingleConsumer) Commit() error {
	_, err := c.kafkaConsumer.Commit()
	return err
}

func (c *SingleConsumer) Close() error {
	// Cancel the context to stop all goroutines
	c.cancelCtx()
	// Close the Kafka consumer
	return c.kafkaConsumer.Close()
}

// Métodos para obtener topic y groupID
func (c *SingleConsumer) Topics() []string {
	return c.topics
}

func (c *SingleConsumer) GroupID() string {
	return c.groupID
}
