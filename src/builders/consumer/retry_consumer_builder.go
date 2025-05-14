package consumer_builder

import (
	"fmt"

	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/consumer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type RetryConsumerBuilder struct {
	bootstrapServers string
	topics           []string
	groupID          string
	autoOffsetReset  string
	enableAutoCommit bool
	producer         types.SingleProducer
	retryInterval    int
	maxRetries       int
}

func NewRetryConsumerBuilder(bootstrapServers string, topics []string, groupID string, retryInterval int) *RetryConsumerBuilder {
	return &RetryConsumerBuilder{
		bootstrapServers: bootstrapServers,
		topics:           topics,
		retryInterval:    retryInterval,
		maxRetries:       consumer.DefaultMaxRetries,
		groupID:          groupID,
	}
}

func (b *RetryConsumerBuilder) SetGroupID(groupID string) *RetryConsumerBuilder {
	b.groupID = groupID
	return b
}

func (b *RetryConsumerBuilder) SetTopics(topics []string) *RetryConsumerBuilder {
	b.topics = topics
	return b
}

func (b *RetryConsumerBuilder) SetProducer(producer types.SingleProducer) *RetryConsumerBuilder {
	b.producer = producer
	return b
}

func (b *RetryConsumerBuilder) SetRetryInterval(seconds int) *RetryConsumerBuilder {
	b.retryInterval = seconds
	return b
}

func (b *RetryConsumerBuilder) SetMaxRetries(maxRetries int) *RetryConsumerBuilder {
	b.maxRetries = maxRetries
	return b
}
func (b *RetryConsumerBuilder) SetAutoOffsetReset(reset consumer_types.AutoOffsetReset) *RetryConsumerBuilder {
	b.autoOffsetReset = string(reset)
	return b
}
func (b *RetryConsumerBuilder) SetEnableAutoCommit(val bool) *RetryConsumerBuilder {
	b.enableAutoCommit = val
	return b
}

func (b *RetryConsumerBuilder) Build() (*consumer.RetryConsumer, error) {
	if b.groupID == "" {
		return nil, fmt.Errorf("group.id es requerido")
	}

	if b.producer == nil {
		return nil, fmt.Errorf("producer es requerido")
	}

	if len(b.topics) == 0 {
		return nil, fmt.Errorf("topics es requerido")
	}

	if b.retryInterval <= 0 {
		return nil, fmt.Errorf("retryInterval debe ser mayor a 0")
	}
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.bootstrapServers,
		"group.id":          b.groupID,
	}
	if b.autoOffsetReset != "" {
		(*config)["auto.offset.reset"] = b.autoOffsetReset
	} else {
		(*config)["auto.offset.reset"] = string(consumer_types.AutoOffsetResetEarliest)
	}
	if b.enableAutoCommit {
		(*config)["enable.auto.commit"] = b.enableAutoCommit
	} else {
		(*config)["enable.auto.commit"] = true
	}

	kafkaConsumer, err := kafka.NewConsumer(config)

	if err != nil {
		return nil, err
	}

	if b.producer == nil {
		return nil, kafka.NewError(kafka.ErrState, "El RetryConsumer requiere un productor para manejar reintentos", false)
	}

	// Crear instancia de RetryConsumer con todos los parámetros configurados
	consumer := consumer.NewRetryConsumer(
		kafkaConsumer,
		b.producer,
		b.bootstrapServers,
		utils.MapConfigMapToOptions(config),
		b.topics,
		b.groupID,
		b.retryInterval,
	)

	// Configuramos el número máximo de reintentos si se especificó
	if b.maxRetries > 0 {
		consumer.SetMaxRetries(b.maxRetries)
	}

	return consumer, nil
}
