package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	consumer_builder "github.com/Angeldadro/Katalyze/src/builders/consumer"
	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	producer_builder "github.com/Angeldadro/Katalyze/src/builders/producer"
	"github.com/Angeldadro/Katalyze/src/types"
)

// Constantes de configuración
const (
	// Usando la dirección externa para poder conectar desde fuera del contenedor
	KafkaExternalAddress = "localhost:9092"
	TestTopic            = "test-retry-topic"
	GroupID              = "test-retry-group"
	RetryInterval        = 5 // 5 segundos entre reintentos
	MaxRetries           = 3 // Máximo 3 reintentos
)

func main() {
	fmt.Printf("Prueba de RetryConsumer con espera de conexión\n")
	fmt.Printf("Conectando a: %s\n", KafkaExternalAddress)
	fmt.Printf("Tópico: %s, Grupo: %s\n", TestTopic, GroupID)
	fmt.Printf("Intervalo de reintento: %d segundos, Máx reintentos: %d\n",
		RetryInterval, MaxRetries)

	// Primero creamos un productor para usar con el RetryConsumer
	fmt.Println("Creando productor para reintentos...")
	producerBuilder := producer_builder.NewSingleProducerBuilder("retry-producer", KafkaExternalAddress)
	producer, err := producerBuilder.Build()
	if err != nil {
		log.Fatalf("Error al crear el productor: %v", err)
	}
	defer producer.Close()

	// Crear el RetryConsumer usando su builder
	fmt.Println("Creando RetryConsumer...")
	builder := consumer_builder.NewRetryConsumerBuilder(
		KafkaExternalAddress,
		[]string{TestTopic},
		GroupID,
		RetryInterval,
	)

	// Configurar opciones adicionales
	builder.SetProducer(producer)
	builder.SetMaxRetries(MaxRetries)
	builder.SetAutoOffsetReset(consumer_types.AutoOffsetResetEarliest)
	builder.SetEnableAutoCommit(true)

	// Construir el RetryConsumer
	retryConsumer, err := builder.Build()
	if err != nil {
		log.Fatalf("Error al crear el RetryConsumer: %v", err)
	}
	defer retryConsumer.Close()

	// Registrar el handler para procesar mensajes
	fmt.Println("Suscribiendo al tópico. El consumidor esperará a estar conectado...")
	start := time.Now()

	var messageCount int = 0

	err = retryConsumer.Subscribe(func(msg types.Message) error {
		messageCount++
		msgValue := string(msg.Value())
		fmt.Printf("Mensaje recibido en tópico %s: %s\n", msg.Topic(), msgValue)

		// Cada tercer mensaje fallará para probar el mecanismo de reintentos
		if messageCount%3 == 0 {
			errorMsg := fmt.Sprintf("Error simulado en mensaje #%d para probar reintentos", messageCount)
			fmt.Printf("Forzando error: %s\n", errorMsg)
			return fmt.Errorf(errorMsg)
		}

		fmt.Printf("Mensaje #%d procesado correctamente\n", messageCount)
		return nil
	})

	if err != nil {
		log.Fatalf("Error al suscribirse: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("¡Conexión establecida en %v!\n", elapsed)
	fmt.Println("RetryConsumer listo y procesando mensajes...")

	// Informar sobre los tópicos de retry y DLQ
	fmt.Printf("Tópico de reintentos: %s\n", retryConsumer.GetRetryTopic())
	fmt.Printf("Tópico de mensajes muertos (DLQ): %s\n", retryConsumer.GetDLQTopic())

	// Configurar canal para recibir señales de interrupción
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Presiona Ctrl+C para terminar...")
	<-sigChan
	fmt.Println("Cerrando aplicación...")
}
