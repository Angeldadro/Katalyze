package producer_builder

import (
	"fmt"

	producer_types "github.com/Angeldadro/Katalyze/src/builders/producer/types"
	"github.com/Angeldadro/Katalyze/src/producer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

// KafkaProducerBuilder para construcción flexible
type SingleProducerBuilder struct {
	Name                             string
	BootstrapServers                 string
	ClientId                         string
	Acks                             producer_types.Acks
	CompressionType                  producer_types.CompressionType
	EnableIdempotence                bool
	MaxInFlightRequestsPerConnection int
	LingerMs                         int
}

func NewSingleProducerBuilder(name, bootstrapServers string) *SingleProducerBuilder {
	return &SingleProducerBuilder{
		Name:             name,
		BootstrapServers: bootstrapServers,
	}
}

func (b *SingleProducerBuilder) SetClientId(clientId string) *SingleProducerBuilder {
	b.ClientId = clientId
	return b
}

func (b *SingleProducerBuilder) SetAcks(acks producer_types.Acks) *SingleProducerBuilder {
	b.Acks = acks
	return b
}

func (b *SingleProducerBuilder) SetCompressionType(compression producer_types.CompressionType) *SingleProducerBuilder {
	b.CompressionType = compression
	return b
}

func (b *SingleProducerBuilder) SetEnableIdempotence(val bool) *SingleProducerBuilder {
	b.EnableIdempotence = val
	return b
}

func (b *SingleProducerBuilder) SetMaxInFlightRequestsPerConnection(val int) *SingleProducerBuilder {
	b.MaxInFlightRequestsPerConnection = val
	return b
}

func (b *SingleProducerBuilder) SetLingerMs(val int) *SingleProducerBuilder {
	b.LingerMs = val
	return b
}

func (b *SingleProducerBuilder) Build() (types.SingleProducer, error) {
	if b.Name == "" {
		return nil, fmt.Errorf("name es requerido")
	}
	if b.BootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap.servers es requerido")
	}
	config := &kafka.ConfigMap{
		"bootstrap.servers": b.BootstrapServers,
	}

	// Configuraciones opcionales
	if b.ClientId != "" {
		(*config)["client.id"] = b.ClientId
	} else {
		(*config)["client.id"] = uuid.New().String()
	}
	if b.Acks != "" {
		(*config)["acks"] = string(b.Acks)
	} else {
		(*config)["acks"] = string(producer_types.AcksAll)
	}
	if b.CompressionType != "" {
		(*config)["compression.type"] = string(b.CompressionType)
	}
	if b.EnableIdempotence {
		(*config)["enable.idempotence"] = b.EnableIdempotence
	}
	if b.MaxInFlightRequestsPerConnection != 0 {
		(*config)["max.in.flight.requests.per.connection"] = b.MaxInFlightRequestsPerConnection
	}
	if b.LingerMs != 0 {
		(*config)["linger.ms"] = b.LingerMs
	}

	kafkaProducer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}
	return producer.NewSingleProducer(b.Name, kafkaProducer, utils.MapConfigMapToOptions(config)), nil
}
