package consumer_helper

import (
	consumer_builder "github.com/Angeldadro/Katalyze/src/builders/consumer"
	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	"github.com/Angeldadro/Katalyze/src/consumer"
	producer_helper "github.com/Angeldadro/Katalyze/src/helpers/producer"
)

// RetryConsumerConfig contiene la configuración simplificada para crear un RetryConsumer
type RetryConsumerConfig struct {
	// Parámetros obligatorios
	BootstrapServers string
	Topics           []string
	GroupID          string
	
	// Parámetros opcionales con valores predeterminados
	RetryInterval    int
	MaxRetries       int
	AutoOffsetReset  consumer_types.AutoOffsetReset
	EnableAutoCommit bool
	ClientID         string
}

// NewDefaultConfig crea una configuración por defecto con valores razonables
func NewDefaultConfig(bootstrapServers string, topics []string, groupID string) RetryConsumerConfig {
	return RetryConsumerConfig{
		BootstrapServers: bootstrapServers,
		Topics:           topics,
		GroupID:          groupID,
		RetryInterval:    consumer.DefaultRetrySeconds,
		MaxRetries:       consumer.DefaultMaxRetries,
		AutoOffsetReset:  consumer_types.AutoOffsetResetEarliest,
		EnableAutoCommit: true,
		ClientID:         "katalyze-retry-consumer",
	}
}

// CreateRetryConsumer crea un RetryConsumer con un productor predeterminado integrado
// Esta función facilita la creación de un RetryConsumer sin tener que manejar
// manualmente la creación y configuración del productor de reintentos necesario
func CreateRetryConsumer(config RetryConsumerConfig) (*consumer.RetryConsumer, error) {
	// 1. Crear un productor optimizado para reintentos
	retryProducer, err := producer_helper.CreateRetryProducer(
		config.BootstrapServers,
		config.ClientID,
	)
	
	if err != nil {
		return nil, err
	}
	
	// 2. Construir el RetryConsumer usando el productor creado
	builder := consumer_builder.NewRetryConsumerBuilder(
		config.BootstrapServers,
		config.Topics,
		config.GroupID,
		config.RetryInterval,
	)
	
	// Configurar el productor creado automáticamente
	builder.SetProducer(retryProducer)
	
	// Aplicar configuraciones adicionales
	builder.SetMaxRetries(config.MaxRetries)
	builder.SetAutoOffsetReset(config.AutoOffsetReset)
	builder.SetEnableAutoCommit(config.EnableAutoCommit)
	
	// Construir y devolver el consumidor
	return builder.Build()
}
