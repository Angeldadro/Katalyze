
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
- **Producers**: Diferentes tipos de productores (Single, Response)
- **Consumers**: Varios tipos de consumidores (Single, Response, Retry)
- **AdminClient**: Para administrar tópicos y recursos de Kafka
- **Builders**: Para construir fácilmente los componentes con configuración fluida
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

### Patrón de Request/Response

```go
// Configurar cliente
client, err := client_builder.NewClientBuilder("my-client")
    .SetBootstrapServers("localhost:9092")
    .Build()
if err != nil {
    log.Fatalf("Error al crear cliente: %v", err)
}

// Crear productor para solicitudes
producer, err := client.CreateResponseProducer("requester", "request-topic")
if err != nil {
    log.Fatalf("Error al crear productor: %v", err)
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
