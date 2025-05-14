package consumer

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	HeaderRetryAt       = "retryat"
	HeaderRetryCount    = "retrycount"
	DefaultRetrySeconds = 5
	DefaultMaxRetries   = 3
)

type RetryConsumer struct {
	kafkaConsumer    *kafka.Consumer
	options          []types.Option
	producer         types.SingleProducer
	topics           []string
	retryTopic       string
	dlqTopic         string
	groupID          string
	ctx              context.Context
	cancelCtx        context.CancelFunc
	retryInterval    int
	maxRetries       int
	bootstrapServers string
}

func NewRetryConsumer(kafkaConsumer *kafka.Consumer, producer types.SingleProducer,
	bootstrapServers string, options []types.Option, topics []string,
	groupID string, retryInterval int) *RetryConsumer {

	if retryInterval <= 0 {
		retryInterval = DefaultRetrySeconds
	}

	ctx, cancel := context.WithCancel(context.Background())

	retryTopic := fmt.Sprintf("%s-%s-retry-%dS", topics[0], groupID, retryInterval)
	dlqTopic := fmt.Sprintf("%s-%s-dlq", topics[0], groupID)

	allTopics := slices.Clone(topics)
	allTopics = append(allTopics, retryTopic)

	err := kafkaConsumer.SubscribeTopics(allTopics, nil)
	if err != nil {
		cancel()
		return nil
	}

	return &RetryConsumer{
		kafkaConsumer:    kafkaConsumer,
		producer:         producer,
		options:          options,
		topics:           allTopics,
		retryTopic:       retryTopic,
		dlqTopic:         dlqTopic,
		groupID:          groupID,
		ctx:              ctx,
		cancelCtx:        cancel,
		retryInterval:    retryInterval,
		maxRetries:       DefaultMaxRetries,
		bootstrapServers: bootstrapServers,
	}
}

func (c *RetryConsumer) Options() []types.Option {
	return c.options
}

func (c *RetryConsumer) Topics() []string {
	return c.topics
}

func (c *RetryConsumer) GroupID() string {
	return c.groupID
}

func (c *RetryConsumer) ReadMessage(ctx context.Context, timeoutMs int) (*message.Message, error) {
	msg, err := c.kafkaConsumer.ReadMessage(time.Duration(timeoutMs) * time.Millisecond)
	if err != nil {
		return nil, err
	}
	return message.NewMessage(msg), nil
}

func (c *RetryConsumer) Commit() error {
	_, err := c.kafkaConsumer.Commit()
	return err
}

func (c *RetryConsumer) Subscribe(handler types.Handler) error {
	go c.consumeMessages(handler)
	return nil
}

func (c *RetryConsumer) consumeMessages(handler types.Handler) {
	running := true
	for running {
		select {
		case <-c.ctx.Done():
			running = false
		default:
			msg, err := c.ReadMessage(c.ctx, 1000)
			if err != nil {
				continue
			}

			if isRetryMessage(msg) {
				c.handleRetryMessage(msg, handler)
			} else {
				c.processMessage(msg, handler)
			}
		}
	}
}

func (c *RetryConsumer) processMessage(msg *message.Message, handler types.Handler) {
	err := handler(msg)
	if err != nil {
		c.scheduleRetry(msg)
	}
}

func (c *RetryConsumer) scheduleRetry(msg *message.Message) {
	retryCount := 0
	retryHeader := utils.GetHeaderFromHeaders(msg.Headers(), HeaderRetryCount)

	// Si ya existe un contador de reintentos, incrementarlo
	if retryHeader != nil {
		count, err := strconv.Atoi(retryHeader.Value())
		if err == nil {
			retryCount = count + 1
		} else {
			retryCount = 1
		}
	} else {
		retryCount = 1
	}

	// Si excedimos el máximo de reintentos, enviamos a DLQ
	if retryCount > c.maxRetries {
		c.sendToDLQ(msg)
		return
	}

	// Calculamos el tiempo de reintento
	retryTime := time.Now().Add(time.Duration(c.retryInterval) * time.Second).UnixNano()
	retryTimeStr := strconv.FormatInt(retryTime, 10)

	headers := []types.Header{
		message.NewHeaderFromKV(HeaderRetryAt, retryTimeStr),
		message.NewHeaderFromKV(HeaderRetryCount, strconv.Itoa(retryCount)),
	}

	// Añadir headers originales excepto retryat y retrycount
	for _, h := range msg.Headers() {
		if h.Key() != HeaderRetryAt && h.Key() != HeaderRetryCount {
			headers = append(headers, h)
		}
	}

	err := c.producer.ProduceWithHeaders(
		c.retryTopic,
		msg.Key(),
		msg.Value(),
		headers,
	)

	if err != nil {
		// Si falla el envío al tópico de retry, enviamos directamente a DLQ
		c.sendToDLQ(msg)
	}
}

func (c *RetryConsumer) handleRetryMessage(msg *message.Message, handler types.Handler) {
	retryAtHeader := utils.GetHeaderFromHeaders(msg.Headers(), HeaderRetryAt)
	if retryAtHeader == nil {
		return
	}

	retryAtStr := retryAtHeader.Value()
	retryAt, err := strconv.ParseInt(retryAtStr, 10, 64)
	if err != nil {
		return
	}

	now := time.Now().UnixNano()
	if now < retryAt {
		// No es tiempo de procesar aún, esperamos
		waitTime := time.Duration(retryAt - now)
		time.Sleep(waitTime)
	}

	// Procesamos el mensaje
	err = handler(msg)
	if err != nil {
		// Si sigue fallando, programamos otro retry
		c.scheduleRetry(msg)
	}
}

func isRetryMessage(msg *message.Message) bool {
	return utils.GetHeaderFromHeaders(msg.Headers(), HeaderRetryAt) != nil
}

// Método para enviar mensajes a la cola de mensajes muertos
func (c *RetryConsumer) sendToDLQ(msg *message.Message) {
	// Preparamos headers adicionales para la DLQ
	headers := []types.Header{
		message.NewHeaderFromKV("dlq_reason", "max_retries_exceeded"),
		message.NewHeaderFromKV("original_topic", msg.Topic()),
		message.NewHeaderFromKV("failed_at", strconv.FormatInt(time.Now().Unix(), 10)),
	}

	// Añadir headers originales
	headers = append(headers, msg.Headers()...)

	c.producer.ProduceWithHeaders(
		c.dlqTopic,
		msg.Key(),
		msg.Value(),
		headers,
	)
}

// GetRetryTopic devuelve el nombre del tópico de reintentos
func (c *RetryConsumer) GetRetryTopic() string {
	return c.retryTopic
}

// GetDLQTopic devuelve el nombre del tópico de mensajes muertos
func (c *RetryConsumer) GetDLQTopic() string {
	return c.dlqTopic
}

// GetMaxRetries devuelve el número máximo de reintentos
func (c *RetryConsumer) GetMaxRetries() int {
	return c.maxRetries
}

// SetMaxRetries establece el número máximo de reintentos
func (c *RetryConsumer) SetMaxRetries(maxRetries int) {
	if maxRetries > 0 {
		c.maxRetries = maxRetries
	}
}

func (c *RetryConsumer) Close() error {
	c.cancelCtx()
	return c.kafkaConsumer.Close()
}
