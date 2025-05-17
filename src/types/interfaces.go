package types

type Option interface {
	Key() string
	Value() interface{}
}

// Producer es la interfaz abstracta para un productor de mensajes.
type Producer interface {
	Name() string
	Options() []Option
	Flush(timeoutMs int) int
	Close() error
}
type SingleProducer interface {
	Producer
	Produce(topic string, key string, value []byte) error
	ProduceWithHeaders(topic string, key []byte, value []byte, headers []Header) error
	// WaitForConnection espera a que la conexión a Kafka esté establecida
	// timeout es el tiempo máximo de espera en milisegundos
	// retryInterval es el intervalo entre intentos en milisegundos
	WaitForConnection(timeout, retryInterval int) error
}
type ResponseProducer interface {
	Producer
	Produce(topic string, key, value []byte, timeoutMs int) ([]byte, error)
	GetReplyTopic() (string, error)
}

type Handler func(msg Message) error

type ResponseHandler func(msg Message) (interface{}, error)

// Consumer es la interfaz abstracta para un consumidor de mensajes.
type Consumer interface {
	Options() []Option
	Topics() []string
	GroupID() string
	Close() error
}
type ResponseConsumer interface {
	Consumer
	Subscribe(handler ResponseHandler) error
}

type SingleConsumer interface {
	Consumer
	Subscribe(handler Handler) error
}

// RetryConsumer es un consumidor que maneja reintentos y cola de mensajes muertos (DLQ)
type RetryConsumer interface {
	SingleConsumer
	GetRetryTopic() string
	GetDLQTopic() string
	GetMaxRetries() int
}

// AdminClient es la interfaz abstracta para administración de clúster.
type AdminClient interface {
	Options() []Option
	CreateTopic(topic string, numPartitions, replicationFactor int) error
	DeleteTopic(topic string) error
	ListTopics() ([]string, error)
	Close() error
}

// is a kafka header
type Header interface {
	Key() string
	Value() string
}

// Message representa un mensaje genérico del sistema de colas.
type Message interface {
	Key() []byte
	Value() []byte
	Topic() string
	Headers() []Header
	Partition() int
	Offset() int64
}

// MessageFuture permite esperar y recibir respuestas asíncronas
type MessageFuture interface {
	ResponseTopic() string
	WaitResponse(timeoutMs int) (Message, error)
	CorrelationID() string
	SetResponse(msg Message)
	Cancel()
}
