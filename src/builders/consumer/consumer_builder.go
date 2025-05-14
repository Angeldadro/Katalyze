package consumer_builder

import (
	"fmt"

	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/consumer"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaConsumerBuilder para construcción flexible
type KafkaConsumerBuilder struct {
	BootstrapServers string
	GroupId          string
	Topics           []string
	AutoOffsetReset  string
	EnableAutoCommit *bool
}

func NewKafkaConsumerBuilder(bootstrapServers, groupId string, topics []string) *KafkaConsumerBuilder {
	return &KafkaConsumerBuilder{
		BootstrapServers: bootstrapServers,
		GroupId:          groupId,
		Topics:           topics,
	}
}

func (b *KafkaConsumerBuilder) SetAutoOffsetReset(reset consumer_types.AutoOffsetReset) *KafkaConsumerBuilder {
	b.AutoOffsetReset = string(reset)
	return b
}
func (b *KafkaConsumerBuilder) SetEnableAutoCommit(val bool) *KafkaConsumerBuilder {
	b.EnableAutoCommit = &val
	return b
}

func (b *KafkaConsumerBuilder) Build() (*consumer.SingleConsumer, error) {
	if b.BootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap.servers es requerido")
	}
	if b.GroupId == "" {
		return nil, fmt.Errorf("group.id es requerido")
	}
	if b.Topics == nil {
		return nil, fmt.Errorf("topics es requerido")
	}
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.BootstrapServers,
		"group.id":          b.GroupId,
	}
	if b.AutoOffsetReset != "" {
		(*config)["auto.offset.reset"] = b.AutoOffsetReset
	} else {
		// Usamos earliest para asegurarnos de capturar todos los mensajes, incluso los antiguos
		(*config)["auto.offset.reset"] = string(consumer_types.AutoOffsetResetEarliest)
	}
	if b.EnableAutoCommit != nil {
		(*config)["enable.auto.commit"] = *b.EnableAutoCommit
	} else {
		(*config)["enable.auto.commit"] = true
	}

	kafkaConsumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	return consumer.NewConsumer(kafkaConsumer, utils.MapConfigMapToOptions(config), b.Topics, b.GroupId), nil
}
