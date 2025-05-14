package examples

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	producer_helper "github.com/Angeldadro/Katalyze/src/helpers/producer"
	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
)

// ExampleMessage es un mensaje para demostración
type ExampleMessage struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Value     string    `json:"value"`
}

// ProducerExample demuestra cómo usar los helpers para crear fácilmente
// productores con diferentes configuraciones predefinidas
func ProducerExample() {
	// Definir la configuración básica para Kafka
	bootstrapServers := "localhost:9092"
	clientID := "mi-aplicacion"
	topic := "mi-topico"

	// Ejemplo 1: Crear un productor con el preset de reintentos
	retryProducer, err := producer_helper.CreateProducer(bootstrapServers, clientID)
	if err != nil {
		log.Fatalf("Error al crear productor para reintentos: %v", err)
	}
	defer retryProducer.Close()

	fmt.Println("Productor para reintentos creado con éxito")

	// Ejemplo 2: Crear un productor para alto rendimiento
	highThroughputProducer, err := producer_helper.CreateDefaultProducer(
		bootstrapServers,
		clientID,
		producer_helper.PresetHighThroughput,
	)
	if err != nil {
		log.Fatalf("Error al crear productor de alto rendimiento: %v", err)
	}
	defer highThroughputProducer.Close()

	fmt.Println("Productor de alto rendimiento creado con éxito")

	// Ejemplo 3: Crear un productor para baja latencia
	lowLatencyProducer, err := producer_helper.CreateDefaultProducer(
		bootstrapServers,
		clientID,
		producer_helper.PresetLowLatency,
	)
	if err != nil {
		log.Fatalf("Error al crear productor de baja latencia: %v", err)
	}
	defer lowLatencyProducer.Close()

	fmt.Println("Productor de baja latencia creado con éxito")

	// Ejemplo de envío de mensaje con headers utilizando los productores creados
	// En este caso usamos el productor de baja latencia para una respuesta rápida
	enviarMensajeEjemplo(lowLatencyProducer, topic)
}

// enviarMensajeEjemplo demuestra cómo enviar un mensaje con headers usando el productor
func enviarMensajeEjemplo(producer types.SingleProducer, topic string) {
	// Crear un mensaje de ejemplo
	msg := ExampleMessage{
		ID:        fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		Timestamp: time.Now(),
		Value:     "Este es un mensaje de ejemplo",
	}

	// Serializar el mensaje a JSON
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error al serializar mensaje: %v", err)
		return
	}

	// Crear algunos headers para el mensaje
	headers := []types.Header{
		message.NewHeaderFromKV("message_id", msg.ID),
		message.NewHeaderFromKV("message_type", "example"),
		message.NewHeaderFromKV("timestamp", fmt.Sprintf("%d", time.Now().Unix())),
	}

	// Enviar el mensaje con los headers
	err = producer.ProduceWithHeaders(
		topic,          // Tópico destino
		[]byte(msg.ID), // Clave del mensaje
		data,           // Valor del mensaje (JSON)
		headers,        // Headers
	)

	if err != nil {
		log.Printf("Error al enviar mensaje: %v", err)
		return
	}

	// Hacer flush para asegurar que el mensaje se envía inmediatamente
	pendingCount := producer.Flush(1000) // 1 segundo de timeout

	if pendingCount > 0 {
		fmt.Printf("Advertencia: %d mensajes aún pendientes después del flush\n", pendingCount)
	} else {
		fmt.Printf("Mensaje enviado correctamente al tópico '%s'\n", topic)
		fmt.Printf("  ID: %s\n", msg.ID)
		fmt.Printf("  Timestamp: %s\n", msg.Timestamp.Format(time.RFC3339))
	}
}
