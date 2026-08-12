package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	orderv1 "event-market/api/gen/v1/order"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Println("Нету кафки брокера")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "order.events",
		GroupID:  "notification-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("[NOTIFICATION-SERVICE] Consumer is running and listening to 'order.events'...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[NOTIFICATION-SERVICE] Shutting down consumer...")
				break
			}
			log.Printf("[ERROR] Failed to read Kafka message: %v", err)
			continue
		}

		var event orderv1.OrderCreatedEvent
		if err := proto.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[ERROR] Failed to unmarshal Protobuf payload: %v", err)
			continue
		}

		// Бизнес-логика: вывод уведомления
		log.Printf("==================================================")
		log.Printf("📧 [NOTIFICATION SENT]")
		log.Printf("   Order ID:     %s", event.GetOrderId())
		log.Printf("   User ID:      %s", event.GetUserId())
		log.Printf("   Total Amount: $%.2f", event.GetTotalAmount())
		log.Printf("   Items Count:  %d", len(event.GetItems()))
		log.Printf("==================================================")
	}
}
