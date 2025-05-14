package examples

import (
	"fmt"
	"log"

	"github.com/Angeldadro/Katalyze/src/helpers/consumer"
	"github.com/Angeldadro/Katalyze/src/types"
)

// RetryConsumerExample demuestra cómo usar los helpers para crear fácilmente
// un RetryConsumer con un productor predeterminado inyectado
func RetryConsumerExample() {
	// Definir la configuración básica para Kafka
	bootstrapServers := "localhost:9092"
	topics := []string{"mi-topico"}
	groupID := "mi-grupo"
	
	// Obtener configuración predeterminada y personalizarla según sea necesario
	config := consumer_helper.NewDefaultConfig(bootstrapServers, topics, groupID)
	
	// Personalización opcional de parámetros adicionales
	config.RetryInterval = 10 // Intervalo de reintento de 10 segundos
	config.MaxRetries = 5     // Máximo 5 reintentos
	config.ClientID = "mi-aplicacion"
	
	// Crear RetryConsumer con un productor predeterminado ya inyectado
	retryConsumer, err := consumer_helper.CreateRetryConsumer(config)
	if err != nil {
		log.Fatalf("Error al crear RetryConsumer: %v", err)
	}
	
	// Esta operación cerraría automáticamente tanto el consumidor como su productor
	defer retryConsumer.Close()
	
	// Ahora podemos usar el consumidor directamente
	fmt.Println("RetryConsumer creado con éxito")
	fmt.Printf("Tópicos configurados: %v\n", retryConsumer.Topics())
	fmt.Printf("Tópico de reintentos: %s\n", retryConsumer.GetRetryTopic())
	fmt.Printf("Tópico DLQ: %s\n", retryConsumer.GetDLQTopic())
	
	// Registrar un handler para procesar mensajes
	retryConsumer.Subscribe(func(msg types.Message) error {
		fmt.Printf("Procesando mensaje: %s\n", string(msg.Value()))
		return nil
	})
}
