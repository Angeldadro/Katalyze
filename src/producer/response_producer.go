package producer

import (
	"fmt"
	"time"

	consumer_builder "github.com/Angeldadro/Katalyze/src/builders/consumer"
	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const (
	HeaderCorrelationID = "correlationId"
	HeaderReplyTo       = "replyTo"
)

// KafkaProducer implementa la interfaz Producer usando confluent-kafka-go
type ResponseProducer struct {
	name              string
	bootstrapServers  string
	kafkaProducer     *kafka.Producer
	responsesConsumer types.SingleConsumer
	replyTopic        string
	options           []types.Option
	futures           *utils.TypedSyncMap[string, types.MessageFuture] // map de futures para manejar las respuestas donde key es el correlationId
}

func NewResponseProducer(name string, bootstrapServers string, kafkaProducer *kafka.Producer, replyTopic string, options []types.Option) (types.ResponseProducer, error) {
	groupID := uuid.NewString()

	consumerBuilder := consumer_builder.NewKafkaConsumerBuilder(
		bootstrapServers,
		groupID,
		[]string{replyTopic},
	)
	consumer, err := consumerBuilder.
		SetEnableAutoCommit(true).
		SetAutoOffsetReset(consumer_types.AutoOffsetResetLatest).
		Build()
	if err != nil {
		return nil, err
	}
	responseProducer := &ResponseProducer{
		name:              name,
		bootstrapServers:  bootstrapServers,
		kafkaProducer:     kafkaProducer,
		replyTopic:        replyTopic,
		options:           options,
		futures:           utils.NewTypedSyncMap[string, types.MessageFuture](),
		responsesConsumer: consumer,
	}
	responseProducer.startConsumer()
	return responseProducer, nil
}
func (p *ResponseProducer) startConsumer() {
	p.responsesConsumer.Subscribe(func(msg types.Message) error {

		// Procesando mensaje de respuesta
		// Verificar si el mensaje tiene headers y procesar solo si hay un correlationID
		var correlationID string
		if len(msg.Headers()) > 0 {
			header := utils.GetHeaderFromHeaders(msg.Headers(), HeaderCorrelationID)
			if header != nil {
				correlationID = header.Value()
			}
		}

		if correlationID == "" {
			return nil
		}

		// Verificar si existe un future para este correlationID
		future, exists := p.futures.Load(correlationID)
		if !exists {
			return nil
		}

		future.SetResponse(msg)
		return nil
	})
}
func (p *ResponseProducer) Name() string {
	return p.name
}
func (p *ResponseProducer) GetReplyTopic() (string, error) {
	return p.replyTopic, nil
}
func (p *ResponseProducer) Produce(topic string, key, value []byte, timeoutMs int) ([]byte, error) {

	correlationID := uuid.NewString()
	future := NewMessageFuture(correlationID, p.replyTopic)
	p.futures.Store(correlationID, future)

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers: []kafka.Header{
			{Key: HeaderCorrelationID, Value: []byte(correlationID)},
			{Key: HeaderReplyTo, Value: []byte(p.replyTopic)},
		},
	}

	if err := p.kafkaProducer.Produce(msg, nil); err != nil {
		return nil, err
	}
	// Aumentar el timeout a 10 segundos para dar suficiente tiempo para recibir respuestas
	res, err := future.WaitResponse(timeoutMs)
	if err != nil {
		fmt.Printf("Error al recibir la respuesta: %v\n", err)
		return nil, err
	}
	future.Cancel()
	return res.Value(), nil
}

func (p *ResponseProducer) Flush(timeoutMs int) int {
	return p.kafkaProducer.Flush(timeoutMs)
}

func (p *ResponseProducer) Close() error {
	p.responsesConsumer.Close()
	p.kafkaProducer.Close()
	return nil
}

func (p *ResponseProducer) Options() []types.Option {
	return p.options
}

type MessageFuture struct {
	correlationID string
	response      chan types.Message
	responseMsg   types.Message

	responseTopic string
	cancelChan    chan struct{}
}

func NewMessageFuture(correlationID string, responseTopic string) *MessageFuture {
	return &MessageFuture{
		correlationID: correlationID,
		responseTopic: responseTopic,
		cancelChan:    make(chan struct{}),
		response:      make(chan types.Message, 1),
	}
}

func (f *MessageFuture) ResponseTopic() string {
	return f.responseTopic
}
func (f *MessageFuture) CorrelationID() string {
	return f.correlationID
}
func (f *MessageFuture) Response() types.Message {
	return f.responseMsg
}
func (f *MessageFuture) SetResponse(msg types.Message) {
	f.responseMsg = msg
	f.response <- msg
}
func (f *MessageFuture) Cancel() {
	close(f.cancelChan)
}
func (f *MessageFuture) WaitResponse(timeoutMs int) (types.Message, error) {
	select {
	case msg := <-f.response:
		return msg, nil
	case <-f.cancelChan:
		return nil, fmt.Errorf("cancelled")
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		return nil, fmt.Errorf("timeout")
	}
}
