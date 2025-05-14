package producer_helper

import (
	"fmt"

	producer_builder "github.com/Angeldadro/Katalyze/src/builders/producer"
	producer_types "github.com/Angeldadro/Katalyze/src/builders/producer/types"
	"github.com/Angeldadro/Katalyze/src/types"
)

// ProducerPreset define el tipo de preset para productores predefinidos
type ProducerPreset string

const (
	// PresetRetry es un productor optimizado para manejar reintentos (buena fiabilidad, compresión alta)
	PresetRetry ProducerPreset = "retry"

	// PresetHighThroughput es un productor optimizado para alto rendimiento (buena velocidad, compresión media)
	PresetHighThroughput ProducerPreset = "high-throughput"

	// PresetLowLatency es un productor optimizado para baja latencia (respuesta rápida, sin compresión)
	PresetLowLatency ProducerPreset = "low-latency"

	// PresetDLQ es un productor optimizado para cola de mensajes muertos (fiabilidad máxima)
	PresetDLQ ProducerPreset = "dlq"

	// DefaultProducerPrefix es el prefijo usado para nombrar productores predeterminados
	DefaultProducerPrefix = "default-producer"
)

// CreateDefaultProducer crea un productor con configuración predeterminada basada en un preset
// Este es un helper simple para crear productores comunes que se pueden inyectar en consumidores
func CreateDefaultProducer(
	bootstrapServers string,
	clientID string,
	preset ProducerPreset,
) (types.SingleProducer, error) {
	// Si no se proporciona un preset, usar el de retry por defecto
	if preset == "" {
		preset = PresetRetry
	}

	producerName := fmt.Sprintf("%s-%s", DefaultProducerPrefix, preset)
	builder := producer_builder.NewSingleProducerBuilder(producerName, bootstrapServers)

	// Configurar ClientID si se proporciona
	if clientID != "" {
		builder.SetClientId(clientID)
	}

	// Aplicar configuraciones específicas según el preset
	switch preset {
	case PresetRetry:
		// Para retry, queremos máxima fiabilidad y buena compresión
		builder.SetAcks(producer_types.AcksAll)
		builder.SetCompressionType(producer_types.CompressionTypeSnappy)
		builder.SetEnableIdempotence(true)
		builder.SetMaxInFlightRequestsPerConnection(5)
		builder.SetLingerMs(10) // Pequeño linger para agrupar mensajes similares

	case PresetHighThroughput:
		// Para alto rendimiento, queremos buena compresión y agrupación
		builder.SetAcks(producer_types.AcksOne)
		builder.SetCompressionType(producer_types.CompressionTypeSnappy)
		builder.SetLingerMs(25) // Mayor linger para agrupar más mensajes
		builder.SetMaxInFlightRequestsPerConnection(10)

	case PresetLowLatency:
		// Para baja latencia, priorizamos velocidad sobre fiabilidad
		builder.SetAcks(producer_types.AcksOne)
		builder.SetCompressionType(producer_types.CompressionTypeNone)
		builder.SetLingerMs(0) // Sin linger para envío inmediato

	case PresetDLQ:
		// Para DLQ, queremos máxima fiabilidad
		builder.SetAcks(producer_types.AcksAll)
		builder.SetCompressionType(producer_types.CompressionTypeSnappy)
		builder.SetEnableIdempotence(true)
		builder.SetMaxInFlightRequestsPerConnection(3) // Menor concurrencia para mayor fiabilidad
		builder.SetLingerMs(5)
	}

	// Construir el productor con la configuración aplicada
	return builder.Build()
}

// CreateRetryProducer crea un productor específicamente optimizado para reintentos
// Esta es una función de conveniencia para crear un productor para RetryConsumer
func CreateRetryProducer(bootstrapServers, clientID string) (types.SingleProducer, error) {
	return CreateDefaultProducer(bootstrapServers, clientID, PresetRetry)
}

func CreateProducer(bootstrapServers, clientID string) (types.SingleProducer, error) {
	return CreateDefaultProducer(bootstrapServers, clientID, "")
}

// CreateDLQProducer crea un productor específicamente optimizado para mensajes DLQ
func CreateDLQProducer(bootstrapServers, clientID string) (types.SingleProducer, error) {
	return CreateDefaultProducer(bootstrapServers, clientID, PresetDLQ)
}
