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
	KafkaAddress = "localhost:9092"
	TestTopic    = "test-response-topic"
	GroupID      = "test-response-group"
)

func main() {
	fmt.Printf("Prueba de ResponseConsumer con espera de conexión\n")
	fmt.Printf("Conectando a: %s\n", KafkaAddress)
	fmt.Printf("Tópico: %s, Grupo: %s\n", TestTopic, GroupID)

	// Primero creamos un productor para usar con el ResponseConsumer
	fmt.Println("Creando productor...")
	producerBuilder := producer_builder.NewSingleProducerBuilder("response-producer", KafkaAddress)
	producer, err := producerBuilder.Build()
	if err != nil {
		log.Fatalf("Error al crear el productor: %v", err)
	}
	defer producer.Close()

	// Crear el ResponseConsumer usando su builder
	fmt.Println("Creando ResponseConsumer...")
	builder := consumer_builder.NewKafkaConsumerBuilder(
		KafkaAddress,
		GroupID,
		[]string{TestTopic},
	)

	// Configurar opciones adicionales
	builder.SetAutoOffsetReset(consumer_types.AutoOffsetResetEarliest)
	builder.SetEnableAutoCommit(true)

	// Construir el ResponseConsumer
	responseConsumer, err := builder.Build()
	if err != nil {
		log.Fatalf("Error al crear el ResponseConsumer: %v", err)
	}
	defer responseConsumer.Close()

	// Registrar el handler para procesar mensajes
	fmt.Println("Suscribiendo al tópico. El consumidor esperará a estar conectado...")
	start := time.Now()

	err = responseConsumer.Subscribe(func(msg types.Message) error {
		msgValue := string(msg.Value())
		fmt.Printf("Mensaje recibido en tópico %s: %s\n", msg.Topic(), msgValue)

		// Devolver una respuesta simple que contenga el mensaje original
		return fmt.Errorf("Error al procesar el mensaje: %s", msgValue)
	})

	if err != nil {
		log.Fatalf("Error al suscribirse: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("¡Conexión establecida en %v!\n", elapsed)
	fmt.Println("ResponseConsumer listo y procesando mensajes...")

	// Configurar canal para recibir señales de interrupción
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Presiona Ctrl+C para terminar...")
	<-sigChan
	fmt.Println("Cerrando aplicación...")
}
