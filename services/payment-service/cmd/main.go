package main

import (
	"context"
	orderv1 "event-market/api/gen/v1/order"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	brokerAddress := os.Getenv("KAFKA_BROKERS")
	if brokerAddress == "" {
		log.Fatal("KAFKA_BROKERS environment variable is not set")
	}

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   "order.events",
		GroupID: "payment-group",
	})
	defer kafkaReader.Close()

	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    "payment.events",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	log.Println("[PAYMENT-SERVICE] Running and listening to 'order.events'...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		msg, err := kafkaReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[PAYMENT-SERVICE] Shutting down payment consumer...")
				break
			}
			log.Printf("[ERROR] Failed to read message: %v", err)
			continue
		}

		var orderEvent orderv1.OrderCreatedEvent
		if err := proto.Unmarshal(msg.Value, &orderEvent); err != nil {
			log.Printf("[ERROR] Failed to unmarshal OrderCreatedEvent: %v", err)
			continue
		}

		paymentID := uuid.New().String()
		paymentEvent := &orderv1.PaymentProcessedEvent{
			OrderId:   orderEvent.GetOrderId(),
			PaymentId: paymentID,
			Success:   true,
		}

		payload, err := proto.Marshal(paymentEvent)
		if err != nil {
			log.Printf("[ERROR] Failed to marshal PaymentProcessedEvent: %v", err)
			continue
		}

		err = kafkaWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(orderEvent.GetOrderId()), // Key выравнивает партиционирование по ID заказа
			Value: payload,
		})
		if err != nil {
			log.Printf("[ERROR] Failed to write message to Kafka: %v", err)
			continue
		}

		log.Printf("💳 [PAYMENT PROCESSED] Order ID: %s | Payment ID: %s | Status: SUCCESS",
			orderEvent.GetOrderId(), paymentID)
	}
}
