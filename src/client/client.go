package client

import (
	"reflect"
	"sync"

	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
)

type Client struct {
	id string

	producers *utils.TypedSyncMap[string, types.Producer]
	consumers *utils.TypedSyncMap[string, map[string][]types.Consumer]

	adminClient types.AdminClient
	closeOnce   sync.Once
	closeWg     sync.WaitGroup
}

func NewClient(id string, adminClient types.AdminClient) *Client {
	return &Client{
		id:          id,
		adminClient: adminClient,
		producers:   utils.NewTypedSyncMap[string, types.Producer](),
		consumers:   utils.NewTypedSyncMap[string, map[string][]types.Consumer](),
		closeOnce:   sync.Once{},
		closeWg:     sync.WaitGroup{},
	}
}

// GetAdmin devuelve la interfaz AdminClient para administración del clúster
func (c *Client) GetAdmin() types.AdminClient {
	return c.adminClient
}

func (c *Client) RegisterProducer(producer types.Producer) error {

	c.producers.Store(producer.Name(), producer)
	return nil
}
func (c *Client) RegisterResponseProducer(producer types.ResponseProducer) error {
	c.producers.Store(producer.Name(), producer)

	// Crear topic si no existe
	if replyTopic, err := producer.GetReplyTopic(); err != nil {
		return err
	} else {
		if err := c.createTopicIfNotExists(replyTopic); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) RegisterConsumer(consumer types.Consumer) error {
	topics := consumer.Topics()

	// Crear topic si no existe
	for _, topic := range topics {
		if err := c.createTopicIfNotExists(topic); err != nil {
			return err
		}
	}
	
	// Verificar si es un RetryConsumer para crear tópicos especiales
	if retryConsumer, ok := consumer.(types.RetryConsumer); ok {
		// Crear tópico de retry si no existe
		retryTopic := retryConsumer.GetRetryTopic()
		if err := c.createTopicIfNotExists(retryTopic); err != nil {
			return err
		}
		
		// Crear tópico de DLQ si no existe
		dlqTopic := retryConsumer.GetDLQTopic()
		if err := c.createTopicIfNotExists(dlqTopic); err != nil {
			return err
		}
	}
	
	// Agregar consumer a la lista de consumidores por topic y groupID
	consumers, ok := c.consumers.Load(consumer.GroupID())
	if !ok {
		consumers = make(map[string][]types.Consumer)
	}
	for _, topic := range topics {
		if _, ok := consumers[topic]; !ok {
			consumers[topic] = []types.Consumer{}
		}

		consumers[topic] = append(consumers[topic], consumer)
	}
	c.consumers.Store(consumer.GroupID(), consumers)
	c.closeWg.Add(1)
	return nil
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		// Primero: Cerramos los recursos (no esperamos el WaitGroup porque causa deadlock)
		
		// Cerrar el administrador
		c.adminClient.Close()
		
		// Cerrar todos los productores
		c.producers.Range(func(key string, value types.Producer) bool {
			value.Close()
			return true
		})
		
		// Usar un mapa para rastrear qué consumidores ya hemos cerrado y evitar cerrarlos múltiples veces
		closedConsumers := make(map[uintptr]bool)
		consumerCount := 0
		
		c.consumers.Range(func(key string, value map[string][]types.Consumer) bool {
			for _, consumers := range value {
				for _, consumer := range consumers {
					// Obtener un puntero único al consumidor para usar como clave del mapa
					pointer := reflect.ValueOf(consumer).Pointer()
					
					// Solo cerrar si no lo hemos cerrado ya
					if !closedConsumers[pointer] {
						consumerCount++
						consumer.Close()
						closedConsumers[pointer] = true
					}
				}
			}
			return true
		})
		
		// Decrementar el contador del WaitGroup por cada consumidor único que cerramos
		for i := 0; i < consumerCount; i++ {
			c.closeWg.Done()
		}
	})
	return nil
}

func (c *Client) createTopicIfNotExists(topic string) error {
	topics, err := c.adminClient.ListTopics()
	if err != nil {
		return err
	}

	// Check if the topic exists in the slice of topics
	topicExists := false
	for _, t := range topics {
		if t == topic {
			topicExists = true
			break
		}
	}

	if !topicExists {
		return c.adminClient.CreateTopic(topic, 1, 1)
	}
	return nil
}
