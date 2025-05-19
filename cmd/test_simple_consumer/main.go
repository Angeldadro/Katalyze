package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Angeldadro/Katalyze/src/consumer"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Constantes de configuración
const (
	KafkaAddress = "localhost:9092"
	TestTopic    = "test-topic"
	GroupID      = "test-group"
)

func main() {
	fmt.Printf("Prueba de SingleConsumer con espera de conexión\n")
	fmt.Printf("Conectando a: %s\n", KafkaAddress)
	fmt.Printf("Tópico: %s, Grupo: %s\n", TestTopic, GroupID)

	// Configuración de Kafka
	config := &kafka.ConfigMap{
		"bootstrap.servers": KafkaAddress,
		"group.id":          GroupID,
		"auto.offset.reset": "earliest",
	}

	// Crear el consumidor nativo de Kafka
	kafkaConsumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Error al crear el consumidor de Kafka: %v", err)
	}
	defer kafkaConsumer.Close()

	// Crear nuestro SingleConsumer
	fmt.Println("Creando SingleConsumer...")
	simpleConsumer := consumer.NewConsumer(
		kafkaConsumer,
		[]types.Option{},
		[]string{TestTopic},
		GroupID,
	)

	// Registrar el handler para procesar mensajes
	fmt.Println("Suscribiendo al tópico. El consumidor esperará a estar conectado...")
	start := time.Now()

	err = simpleConsumer.Subscribe(func(msg types.Message) error {
		fmt.Printf("Mensaje recibido en tópico %s: %s\n", msg.Topic(), string(msg.Value()))
		return nil
	})

	if err != nil {
		log.Fatalf("Error al suscribirse: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("¡Conexión establecida en %v!\n", elapsed)
	fmt.Println("Consumidor listo y procesando mensajes...")

	// Configurar canal para recibir señales de interrupción
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Presiona Ctrl+C para terminar...")
	<-sigChan
	fmt.Println("Cerrando aplicación...")
}
