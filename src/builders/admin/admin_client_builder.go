package admin_builder

import (
	"fmt"

	"github.com/Angeldadro/Katalyze/src/admin"
	admin_types "github.com/Angeldadro/Katalyze/src/builders/admin/types"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type KafkaAdminClientBuilder struct {
	BootstrapServers string

	SecurityProtocol admin_types.SecurityProtocol
	RequestTimeoutMs int
	SessionTimeoutMs int
	ClientId         string
}

func NewKafkaAdminClientBuilder(bootstrapServers string) *KafkaAdminClientBuilder {
	return &KafkaAdminClientBuilder{
		BootstrapServers: bootstrapServers,
	}
}

func (b *KafkaAdminClientBuilder) SetSecurityProtocol(protocol admin_types.SecurityProtocol) *KafkaAdminClientBuilder {
	b.SecurityProtocol = protocol
	return b
}

func (b *KafkaAdminClientBuilder) SetRequestTimeoutMs(val int) *KafkaAdminClientBuilder {
	b.RequestTimeoutMs = val
	return b
}

func (b *KafkaAdminClientBuilder) SetSessionTimeoutMs(val int) *KafkaAdminClientBuilder {
	b.SessionTimeoutMs = val
	return b
}

func (b *KafkaAdminClientBuilder) SetClientId(clientId string) *KafkaAdminClientBuilder {
	b.ClientId = clientId
	return b
}

func (b *KafkaAdminClientBuilder) Build() (*admin.KafkaAdminClient, error) {
	if b.BootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap.servers es requerido")
	}

	config := &kafka.ConfigMap{"bootstrap.servers": b.BootstrapServers}
	if b.SecurityProtocol != "" {
		(*config)["security.protocol"] = string(b.SecurityProtocol)
	}
	if b.RequestTimeoutMs != 0 {
		(*config)["request.timeout.ms"] = b.RequestTimeoutMs
	}
	if b.SessionTimeoutMs != 0 {
		(*config)["session.timeout.ms"] = b.SessionTimeoutMs
	}
	if b.ClientId != "" {
		(*config)["client.id"] = b.ClientId
	} else {
		(*config)["client.id"] = "kafka-admin-client-" + uuid.NewString()
	}

	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	adminClientInterface, err := admin.NewKafkaAdminClient(adminClient, []types.Option{})
	if err != nil {
		adminClient.Close()
		return nil, fmt.Errorf("failed to create admin client interface: %w", err)
	}

	return adminClientInterface, nil
}
