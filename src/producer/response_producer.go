package producer

import (
	"fmt"
	"time"

	consumer_builder "github.com/Angeldadro/Katalyze/src/builders/consumer"
	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.comcom/google/uuid"
)

const (
	HeaderCorrelationID = "correlationId"
	HeaderReplyTo       = "replyTo"
)

// ResponseProducer implementa la interfaz Producer usando confluent-kafka-go
type ResponseProducer struct {
	name              string
	bootstrapServers  string
	kafkaProducer     *kafka.Producer
	responsesConsumer types.SingleConsumer
	replyTopic        string
	options           []types.Option
	futures           *utils.TypedSyncMap[string, types.MessageFuture]
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

// --- BLOQUE AÑADIDO ---
// WaitForConnection garantiza que tanto el productor como el consumidor interno
// estén conectados y listos para operar.
func (p *ResponseProducer) WaitForConnection(timeoutMs int) error {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	start := time.Now()

	// 1. Esperar al productor
	// Usamos la técnica de `Flush` para verificar la conectividad del productor.
	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout esperando conexión del productor interno en ResponseProducer")
		}
		// Flush con un timeout pequeño. Si devuelve 0, significa que no hay mensajes
		// pendientes y la conexión está establecida.
		if pending := p.kafkaProducer.Flush(100); pending == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 2. Esperar al consumidor de respuestas
	// El tiempo restante del timeout general se usa para el consumidor.
	remainingTimeout := timeout - time.Since(start)
	if remainingTimeout <= 0 {
		return fmt.Errorf("timeout agotado antes de conectar el consumidor de respuestas")
	}

	// Reutilizamos la lógica robusta de WaitForConnection del SingleConsumer.
	// El tipo concreto de p.responsesConsumer es *consumer.SingleConsumer
	if singleConsumer, ok := p.responsesConsumer.(interface{ WaitForConnection(int) error }); ok {
		return singleConsumer.WaitForConnection(int(remainingTimeout.Milliseconds()))
	}

	return fmt.Errorf("el consumidor de respuestas no implementa WaitForConnection")
}

// --- FIN DEL BLOQUE AÑADIDO ---

func (p *ResponseProducer) startConsumer() {
	p.responsesConsumer.Subscribe(func(msg types.Message) error {
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
