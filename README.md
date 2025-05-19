
# Katalyze

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Version](https://img.shields.io/badge/go-%3E%3D%201.18-blue.svg)

**Katalyze** es un cliente Kafka moderno, robusto y con patrones de diseño avanzados para Go. Diseñado para simplificar el trabajo con Apache Kafka mientras proporciona características avanzadas como reintentos automáticos, mensajería con respuesta, colas de mensajes muertos (DLQ) y más.

## 🚀 Características principales

- ✅ **Patrones de diseño robustos**: Basado en interfaces, builders y dependency injection
- ✅ **Sistema avanzado de reintentos**: RetryConsumer configurable
- ✅ **Colas de mensajes muertos (DLQ)**: Manejo automático de mensajes fallidos
- ✅ **Request/Response**: Implementación de patrones solicitud-respuesta
- ✅ **Cerrado de recursos controlado**: Manejo apropiado de cancelación de contextos
- ✅ **Configuración por código**: Interfaces fluidas con builders para todos los componentes
- ✅ **Helpers predefinidos**: Productores y consumidores con configuraciones optimizadas

## 📦 Instalación

```bash
go get github.com/Angeldadro/Katalyze
```

## 🧩 Componentes principales

- **Client**: Punto de entrada principal para administrar productores y consumidores
  - Registro automático de tópicos al registrar productores y consumidores
  - Gestión de ciclo de vida para recursos Kafka
- **Producers**: Diferentes tipos de productores (Single, Response)
- **Consumers**: Varios tipos de consumidores (Single, Response, Retry)
- **AdminClient**: Para administrar tópicos y recursos de Kafka
  - Creación, eliminación y listado de tópicos
  - Gestión centralizada de recursos del clúster
- **Builders**: Para construir fácilmente los componentes con configuración fluida
  - ClientBuilder: Configuración del cliente principal
  - ProducerBuilder, ConsumerBuilder: Configuración fluida para componentes
- **Helpers**: Ayudantes para crear productores y consumidores con configuraciones predeterminadas

## 🔰 Ejemplos de uso

### Creación de un productor simple

```go
// Usando el builder directamente
producerBuilder := producer_builder.NewSingleProducerBuilder("my-producer", "localhost:9092")
producerBuilder.SetClientId("my-app")
producerBuilder.SetCompressionType(producer_types.CompressionTypeSnappy)

producer, err := producerBuilder.Build()
if err != nil {
    log.Fatalf("Error al crear productor: %v", err)
}
defer producer.Close()

// Enviar mensaje
err = producer.Produce("my-topic", "message-key", []byte("Hello Kafka!"))

// O usando helpers para crear productores predefinidos
producer, err := producer_helper.CreateDefaultProducer(
    "localhost:9092",
    "my-client",
    producer_helper.PresetHighThroughput,
)
```

### Creación de un consumidor con reintentos

```go
// Usando el helper (forma recomendada)
config := consumer_helper.NewDefaultConfig("localhost:9092", []string{"my-topic"}, "my-group")
config.RetryInterval = 10 // segundos
config.MaxRetries = 3

retryConsumer, err := consumer_helper.CreateRetryConsumer(config)
if err != nil {
    log.Fatalf("Error al crear el consumidor: %v", err)
}
defer retryConsumer.Close()

// Suscribirse para procesar mensajes
retryConsumer.Subscribe(func(msg types.Message) error {
    fmt.Printf("Recibido mensaje: %s\n", string(msg.Value()))
    
    // Si devolvemos error, el mensaje irá a la cola de reintentos
    // Después de MaxRetries intentos fallidos, irá a la DLQ
    return nil
})
```

### Registro de tópicos y administración con Client

```go
// Crear AdminClient para gestionar tópicos y recursos Kafka
adminConfig := kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
}

kafkaAdminClient, err := kafka.NewAdminClient(&adminConfig)
if err != nil {
    log.Fatalf("Error creando AdminClient de Kafka: %v", err)
}

// Inicializar nuestro AdminClient personalizado
adminClient, err := admin.NewKafkaAdminClient(kafkaAdminClient, nil)
if err != nil {
    log.Fatalf("Error creando AdminClient: %v", err)
}

// Configurar cliente usando ClientBuilder
client, err := client_builder.NewClientBuilder().
    SetClientId("my-client").
    SetAdminClient(adminClient).
    Build()
if err != nil {
    log.Fatalf("Error al crear cliente: %v", err)
}

// Registrar productor - el cliente creará automáticamente los tópicos necesarios
producer, err := producer_helper.CreateDefaultProducer("localhost:9092", "my-producer-id", producer_helper.PresetDefault)
if err != nil {
    log.Fatalf("Error al crear productor: %v", err)
}

// Registrar productor con el cliente - esto creará el tópico si no existe
err = client.RegisterProducer(producer)
if err != nil {
    log.Fatalf("Error al registrar productor: %v", err)
}

// Para consumidores con retries, todos los tópicos se crean automáticamente
retryConsumer, err := consumer_helper.CreateRetryConsumer(config)
if err != nil {
    log.Fatalf("Error al crear RetryConsumer: %v", err)
}

// Registrar consumidor - se crearán automáticamente:
// - Tópicos principales
// - Tópico de reintentos
// - Tópico DLQ
err = client.RegisterConsumer(retryConsumer)
if err != nil {
    log.Fatalf("Error al registrar consumidor: %v", err)
}
```

### Patrón de Request/Response

```go
// Configurar cliente
client, err := client_builder.NewClientBuilder().
    SetClientId("my-client").
    SetAdminClient(adminClient).
    Build()
if err != nil {
    log.Fatalf("Error al crear cliente: %v", err)
}

// Crear productor para solicitudes
producer, err := producer_helper.CreateResponseProducer("localhost:9092", "requester", "request-topic")
if err != nil {
    log.Fatalf("Error al crear productor: %v", err)
}

// Registrar con el cliente - se crearán los tópicos automáticamente
err = client.RegisterResponseProducer(producer)
if err != nil {
    log.Fatalf("Error al registrar productor: %v", err)
}

// Enviar solicitud y esperar respuesta (con timeout)
data := []byte(`{"name":"John","action":"getUser"}`)
response, err := producer.Produce("request-topic", []byte("user-1"), data, 5000)
if err != nil {
    log.Fatalf("Error en la solicitud: %v", err)
}

fmt.Printf("Respuesta recibida: %s\n", string(response))
```

## 🧰 Modelo de configuración flexible

Katalyze usa builders para facilitar la configuración de todos sus componentes. Esto permite una API fluida y autoexplicativa:

```go
// Ejemplo de configuración fluida con builders
builder := consumer_builder.NewRetryConsumerBuilder(
    bootstrapServers,
    []string{"my-topic"},
    "my-group",
    5, // retry interval
)

builder.SetMaxRetries(3)
    .SetAutoOffsetReset(consumer_types.AutoOffsetResetEarliest)
    .SetProducer(myProducer)

consumer, err := builder.Build()
```

## 🔄 Sistema de reintentos y DLQ

El `RetryConsumer` ofrece reintentos automáticos para mensajes fallidos:

1. Cuando un mensaje falla (handler devuelve error), se envía a un topic de reintentos
2. El mensaje se encolará con un tiempo de retraso configurable
3. Después de exceder el número máximo de reintentos, el mensaje va a la DLQ
4. Los tópicos de reintentos y DLQ se crean automáticamente

```
                  ┌───────────────┐
                  │    Handler    │
                  │   Procesado   │
                  │  Exitoso ✓   │
                  └───────────────┘
                         ▲
                         │
┌───────────┐     ┌───────────────┐     ┌───────────────┐
│   Topic   │────▶│ RetryConsumer │────▶│     DLQ       │
│ Principal │     │               │     │               │
└───────────┘     └───────────────┘     └───────────────┘
                         │  ▲                    
                         │  │                    
                         ▼  │                    
                  ┌───────────────┐     
                  │  Retry Topic  │   
                  └───────────────┘    
```

## 📊 Administración de tópicos

El componente `AdminClient` proporciona una interfaz para gestionar tópicos en Kafka:

```go
// Obtener la instancia AdminClient desde un cliente existente
adminClient := client.GetAdmin()

// Crear un tópico manualmente (particiones, factor de replicación)
err := adminClient.CreateTopic("new-topic", 3, 1)
if err != nil {
    log.Fatalf("Error al crear tópico: %v", err)
}

// Listar tópicos existentes
topics, err := adminClient.ListTopics()
if err != nil {
    log.Fatalf("Error al listar tópicos: %v", err)
}
fmt.Printf("Tópicos disponibles: %v\n", topics)

// Eliminar un tópico
err = adminClient.DeleteTopic("topic-to-delete")
if err != nil {
    log.Fatalf("Error al eliminar tópico: %v", err)
}
```

Nota: El cliente principal (`Client`) utiliza `AdminClient` internamente para gestionar automáticamente la creación de tópicos cuando se registran productores y consumidores, incluyendo tópicos especiales para reintentos y DLQ.

## 🤝 Contribuir

Las contribuciones son bienvenidas. Puedes:

1. Reportar bugs o solicitar características a través de issues
2. Enviar Pull Requests con mejoras
3. Mejorar la documentación
4. Compartir ejemplos de uso

## 📜 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo LICENSE para más detalles.

---

Desarrollado con ❤️ por [@Angeldadro](https://github.com/Angeldadro)
