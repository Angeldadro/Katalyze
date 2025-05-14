package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	admin_builder "github.com/Angeldadro/Katalyze/src/builders/admin"
	client_builder "github.com/Angeldadro/Katalyze/src/builders/client"
	consumer_builder "github.com/Angeldadro/Katalyze/src/builders/consumer"
	consumer_types "github.com/Angeldadro/Katalyze/src/builders/consumer/types"
	producer_builder "github.com/Angeldadro/Katalyze/src/builders/producer"
	producer_types "github.com/Angeldadro/Katalyze/src/builders/producer/types"
	"github.com/Angeldadro/Katalyze/src/message"
	"github.com/Angeldadro/Katalyze/src/types"
	"github.com/Angeldadro/Katalyze/src/utils"
)

const (
	// Direcciones de Kafka según docker-compose.yml
	KafkaExternalAddress = "localhost:9092" // Para conexiones desde fuera del contenedor
	KafkaInternalAddress = "kafka:29092"    // Para conexiones entre contenedores

	// Configuración del cliente y tópicos
	ClientID        = "katalyze-client"
	RequestTopic    = "calculator-requests"
	ConsumerGroupID = "calculator-group"
	ProducerID      = "calculator-producer"

	// Configuración para RetryConsumer
	RetryInterval = 5 // segundos
	MaxRetries    = 3
)

type SumRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type SumResponse struct {
	Result int `json:"result"`
}

type KafkaHeaderWrapper struct {
	key   string
	value string
}

func (h *KafkaHeaderWrapper) Key() string {
	return h.key
}

func (h *KafkaHeaderWrapper) Value() string {
	return h.value
}

// messageHandler maneja los mensajes recibidos por el RetryConsumer
func messageHandler(msg types.Message) error {
	// Imprimir información del tópico y headers
	fmt.Println("====================================================")
	fmt.Printf("🔹 Recibido mensaje desde tópico '%s'\n", msg.Topic())
	fmt.Printf("🔹 Headers detectados: %d\n", len(msg.Headers()))

	// Decodificar el mensaje JSON
	var request SumRequest
	if err := json.Unmarshal(msg.Value(), &request); err != nil {
		fmt.Printf("❌ Error al deserializar mensaje: %v\n", err)
		return err
	}

	// Verificar si es un reintento
	retryCountHeader := utils.GetHeaderFromHeaders(msg.Headers(), "retrycount")
	retryCount := 0
	if retryCountHeader != nil {
		count, _ := strconv.Atoi(retryCountHeader.Value())
		retryCount = count
		fmt.Printf("🔄 REINTENTO #%d detectado\n", retryCount)
	}

	// Extraer ID de mensaje para seguimiento
	msgID := "desconocido"
	msgIDHeader := utils.GetHeaderFromHeaders(msg.Headers(), "message_id")
	if msgIDHeader != nil {
		msgID = msgIDHeader.Value()
	}
	fmt.Printf("🆔 ID del mensaje: %s\n", msgID)

	// Listar todos los headers para depuración
	fmt.Println("📝 Headers completos:")
	for _, h := range msg.Headers() {
		fmt.Printf("   - %s: %s\n", h.Key(), h.Value())
	}

	// Calcular la suma
	result := request.A + request.B
	fmt.Printf("🧮 Operación: %d + %d = %d\n", request.A, request.B, result)

	// Forzar error para números impares (suma impar)
	if result%2 == 1 {
		// Si es la suma igual a 15, fallar siempre hasta llegar a DLQ
		if result == 15 {
			fmt.Printf("❌ ERROR PERSISTENTE: Mensaje %s (suma=15) SIEMPRE fallará\n", msgID)
			return fmt.Errorf("error persistente forzado para suma=15")
		}

		// Para otras sumas impares, fallar en los primeros dos intentos
		if retryCount < 2 {
			fmt.Printf("⚠️ ERROR TEMPORAL: Mensaje %s fallará en intento #%d\n", msgID, retryCount+1)
			return fmt.Errorf("error temporal - fallará hasta el tercer intento")
		}

		// En el tercer intento, dejamos que pase
		fmt.Printf("✅ ÉXITO EN TERCER INTENTO: Mensaje %s con suma=%d\n", msgID, result)
	} else {
		fmt.Printf("✅ ÉXITO INMEDIATO: Mensaje %s con suma=%d (par)\n", msgID, result)
	}

	// No hay error, el mensaje se procesa correctamente
	fmt.Println("====================================================")
	return nil
}

func main() {
	// Configurar canal para manejo de señales de terminación
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Crear cliente de administración
	adminClientBuilder := admin_builder.NewKafkaAdminClientBuilder(KafkaExternalAddress)
	adminClientBuilder.SetClientId(ClientID)
	adminClient, err := adminClientBuilder.Build()
	if err != nil {
		fmt.Printf("Error al crear adminClient: %v\n", err)
		return
	}

	// Crear cliente principal
	clientbuilder := client_builder.NewClientBuilder()
	clientbuilder.SetAdminClient(adminClient)
	clientbuilder.SetClientId(ClientID)
	client, err := clientbuilder.Build()
	if err != nil {
		fmt.Printf("Error al crear client: %v\n", err)
		return
	}

	fmt.Println("Iniciando aplicación con SingleProducer y RetryConsumer...")

	// 1. Crear SingleProducer
	producerBuilder := producer_builder.NewSingleProducerBuilder(ProducerID, KafkaExternalAddress)
	producerBuilder.SetClientId(ClientID)
	producerBuilder.SetAcks(producer_types.AcksAll)
	producerBuilder.SetCompressionType(producer_types.CompressionTypeSnappy) // Mejor compresión

	// Construir el productor
	singleProducer, err := producerBuilder.Build()
	if err != nil {
		fmt.Printf("Error al crear single producer: %v\n", err)
		return
	}

	// Registrar el productor con el cliente
	err = client.RegisterProducer(singleProducer)
	if err != nil {
		fmt.Printf("Error al registrar el productor: %v\n", err)
		return
	}
	fmt.Println("SingleProducer creado y registrado correctamente")

	// 2. Crear RetryConsumer
	retryConsumerBuilder := consumer_builder.NewRetryConsumerBuilder(
		KafkaExternalAddress,
		[]string{RequestTopic},
		ConsumerGroupID,
		RetryInterval,
	)

	// Configurar RetryConsumer
	retryConsumerBuilder.SetProducer(singleProducer)
	retryConsumerBuilder.SetAutoOffsetReset(consumer_types.AutoOffsetResetEarliest)
	retryConsumerBuilder.SetMaxRetries(MaxRetries)

	// Construir el consumidor con retries
	retryConsumer, err := retryConsumerBuilder.Build()
	if err != nil {
		fmt.Printf("Error al crear retry consumer: %v\n", err)
		return
	}

	// Registrar el consumidor con el cliente (esto registrará también los tópicos especiales)
	err = client.RegisterConsumer(retryConsumer)
	if err != nil {
		fmt.Printf("Error al registrar el consumidor: %v\n", err)
		return
	}

	// 3. Suscribir el consumidor al handler
	err = retryConsumer.Subscribe(messageHandler)
	if err != nil {
		fmt.Printf("Error al suscribir el handler: %v\n", err)
		return
	}

	fmt.Println("RetryConsumer creado y registrado correctamente")
	fmt.Printf("Tópicos creados:\n- Principal: %s\n- Retry: %s\n- DLQ: %s\n",
		RequestTopic,
		retryConsumer.GetRetryTopic(),
		retryConsumer.GetDLQTopic(),
	)

	// 4. Preparar y enviar algunos mensajes de prueba
	fmt.Println("Enviando mensajes de prueba...")

	// Creamos exactamente 4 mensajes para probar escenarios específicos
	messages := []SumRequest{
		{A: 8, B: 6},  // 14 (par) - Se procesará correctamente
		{A: 10, B: 5}, // 15 (impar) - Irá a la DLQ después de tres intentos
		{A: 5, B: 2},  // 7 (impar) - Será exitoso en el tercer intento
		{A: 9, B: 12}, // 21 (impar) - Será exitoso en el tercer intento
	}

	// Enviar los mensajes
	for i, req := range messages {
		data, _ := json.Marshal(req)

		// Crear encabezados para identificar el mensaje
		headers := []types.Header{
			message.NewHeaderFromKV("message_id", fmt.Sprintf("msg-%d", i)),
			message.NewHeaderFromKV("timestamp", fmt.Sprintf("%d", time.Now().Unix())),
		}

		// Enviar mensaje con encabezados
		err = singleProducer.ProduceWithHeaders(
			RequestTopic,
			[]byte(fmt.Sprintf("key-%d", i)),
			data,
			headers,
		)

		if err != nil {
			fmt.Printf("Error al enviar mensaje %d: %v\n", i, err)
		} else {
			fmt.Printf("Mensaje %d enviado: %d + %d\n", i, req.A, req.B)
		}

		// Breve pausa entre mensajes
		time.Sleep(500 * time.Millisecond)
	}

	// Flush para asegurar entrega de todos los mensajes
	pendingMsgs := singleProducer.Flush(5000)
	if pendingMsgs > 0 {
		fmt.Printf("Advertencia: %d mensajes aún pendientes después del flush\n", pendingMsgs)
	}

	fmt.Println("Todos los mensajes enviados. Esperando procesamiento...")

	// Mantener la aplicación en ejecución para ver el procesamiento de mensajes y retries
	fmt.Println("Presiona Ctrl+C para salir")

	// Esperar señal de terminación
	<-sigchan

	// Cerrar recursos con timeout
	fmt.Println("\nCerrando recursos...")

	// Establecer un timeout global para el cierre de la aplicación
	shutdownTimeout := 10 * time.Second

	// 1. Primero, detener el consumidor para evitar procesar más mensajes
	fmt.Println("- Deteniendo consumidores...")
	// Creamos un contexto con timeout para esta fase
	consumerCtx, consumerCancel := context.WithTimeout(context.Background(), shutdownTimeout/3)
	ccDone := make(chan struct{})
	go func() {
		// Llamada al Close del retry consumer (que debería ahora respetar el contexto)
		retryConsumer.Close()
		close(ccDone)
	}()

	// Esperar a que se complete el cierre de consumidores o timeout
	select {
	case <-ccDone:
		fmt.Println("  ✓ Consumidores detenidos correctamente")
	case <-consumerCtx.Done():
		fmt.Println("  ⚠️ Timeout al detener consumidores, continuando con el cierre...")
	}
	consumerCancel() // limpiar recursos

	// 2. Luego cerrar el productor para evitar envío de nuevos mensajes
	fmt.Println("- Cerrando productores...")
	producerCtx, producerCancel := context.WithTimeout(context.Background(), shutdownTimeout/3)
	pcDone := make(chan struct{})
	go func() {
		singleProducer.Close()
		close(pcDone)
	}()

	// Esperar a que se complete el cierre del productor o timeout
	select {
	case <-pcDone:
		fmt.Println("  ✓ Productores cerrados correctamente")
	case <-producerCtx.Done():
		fmt.Println("  ⚠️ Timeout al cerrar productores, continuando con el cierre...")
	}
	producerCancel() // limpiar recursos

	// 3. Finalmente cerrar el cliente que maneja las conexiones administrativas
	fmt.Println("- Cerrando cliente Kafka...")
	clientCtx, clientCancel := context.WithTimeout(context.Background(), shutdownTimeout/3)
	clDone := make(chan struct{})
	go func() {
		client.Close()
		close(clDone)
	}()

	// Esperar a que se complete el cierre del cliente o timeout
	select {
	case <-clDone:
		fmt.Println("  ✓ Cliente cerrado correctamente")
	case <-clientCtx.Done():
		fmt.Println("  ⚠️ Timeout al cerrar cliente, finalizando...")
	}
	clientCancel() // limpiar recursos

	fmt.Println("Aplicación terminada correctamente")

	fmt.Println("Hasta pronto! 👋")
}
