package consumer

import (
	"context"
	"log"
	"time"

	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	err := kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Println(err)
	}
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

func (c *SingleConsumer) Subscribe(handler types.Handler) error {
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
