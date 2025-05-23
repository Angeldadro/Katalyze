package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Constantes para los headers (deben coincidir con las de producer)
const (
	HeaderCorrelationID = "correlationId"
	HeaderReplyTo       = "replyTo"
)

// ResponseConsumer implementa la interfaz types.ResponseConsumer para consumo de mensajes con respuesta
type ResponseConsumer struct {
	kafkaConsumer    *kafka.Consumer
	options          []types.Option
	bootstrapServers string
	responseProducer types.SingleProducer
	topics           []string
	groupID          string
	ctx              context.Context
	cancelCtx        context.CancelFunc
}

// NewResponseConsumer crea un nuevo ResponseConsumer
func NewResponseConsumer(kafkaConsumer *kafka.Consumer, responseProducer types.SingleProducer, bootstrapServers string, options []types.Option, topics []string, groupID string) *ResponseConsumer {
	if len(topics) > 1 {
		log.Println("Response consumers should only consume from one topic")
		return nil
	}

	if groupID == "" {
		groupID = topics[0]
	} else {
		fmt.Println("Response producers shoud have a shared groupID between all of them using the kafka load balancing system to prevent duplicated responses, by default katalyze use the topic name as groupID to avoid duplicated responses")
	}

	// Create context with cancelation
	ctx, cancel := context.WithCancel(context.Background())

	// Crear la instancia del consumidor
	consumer := &ResponseConsumer{
		kafkaConsumer:    kafkaConsumer,
		options:          options,
		bootstrapServers: bootstrapServers,
		responseProducer: responseProducer,
		topics:           topics,
		groupID:          groupID,
		ctx:              ctx,
		cancelCtx:        cancel,
	}

	return consumer
}

// Options retorna las opciones de configuración del consumidor
func (c *ResponseConsumer) Options() []types.Option {
	return c.options
}

// Topics retorna los tópicos a los que está suscrito el consumidor
func (c *ResponseConsumer) Topics() []string {
	return c.topics
}

// GroupID retorna el ID del grupo de consumidores
func (c *ResponseConsumer) GroupID() string {
	return c.groupID
}

// WaitForConnection espera a que el consumidor esté conectado a Kafka
// timeout es la duración máxima a esperar en milisegundos
// retorna error si no se puede conectar en el tiempo especificado
func (c *ResponseConsumer) WaitForConnection(timeout int) error {
	if c.kafkaConsumer == nil {
		return errors.New("kafka consumer no inicializado")
	}

	// Primero intenta suscribirse a los tópicos
	err := c.kafkaConsumer.SubscribeTopics(c.topics, nil)
	if err != nil {
		return err
	}

	// Define un contexto con timeout para limitar el tiempo de espera
	timeoutDuration := time.Duration(timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	// Intervalo de verificación
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout esperando la conexión a Kafka")
		case <-tick.C:
			// Intenta obtener los metadatos para comprobar la conexión
			metadata, err := c.kafkaConsumer.GetMetadata(nil, true, int(timeoutDuration.Milliseconds()))
			if err == nil && len(metadata.Brokers) > 0 {
				// Conectado exitosamente
				log.Println("ResponseConsumer conectado a Kafka exitosamente")
				return nil
			}
		}
	}
}

// Subscribe inicia la suscripción a los tópicos con un handler para procesar las respuestas
func (c *ResponseConsumer) Subscribe(handler types.ResponseHandler) error {
	// Esperar a que esté conectado antes de iniciar el consumo
	err := c.WaitForConnection(30000) // 30 segundos de timeout
	if err != nil {
		return err
	}

	go func() {
		running := true
		for running {
			select {
			case <-c.ctx.Done():
				running = false
			default:
				msg, err := c.ReadMessage(c.ctx, 2000)
				if err != nil {
					continue
				}
				response, err := handler(msg)
				if err != nil {
					log.Printf("Error al procesar la respuesta: %v", err)
					continue
				}
				if response != nil {
					marshaled, err := json.Marshal(response)
					if err != nil {
						log.Printf("Error al serializar la respuesta: %v", err)
						continue
					}

					correlationID := utils.GetHeaderFromHeaders(msg.Headers(), HeaderCorrelationID)
					replyTo := utils.GetHeaderFromHeaders(msg.Headers(), HeaderReplyTo)

					if replyTo == nil || correlationID == nil {
						continue
					}

					headers := []types.Header{message.NewHeaderFromKV(HeaderCorrelationID, correlationID.Value())}
					if err := c.responseProducer.ProduceWithHeaders(replyTo.Value(), []byte(correlationID.Value()), marshaled, headers); err != nil {
						log.Printf("Error al enviar la respuesta: %v", err)
					}
				}
			}
		}
	}()
	return nil
}

// ReadMessage lee un mensaje y lo convierte al formato Message
func (c *ResponseConsumer) ReadMessage(ctx context.Context, timeoutMs int) (*message.Message, error) {
	msg, err := c.kafkaConsumer.ReadMessage(time.Duration(timeoutMs) * time.Millisecond)
	if err != nil {
		return nil, err
	}
	return message.NewMessage(msg), nil
}

// Close cierra el consumidor y libera los recursos
func (c *ResponseConsumer) Close() error {
	c.cancelCtx()
	return c.kafkaConsumer.Close()
}
