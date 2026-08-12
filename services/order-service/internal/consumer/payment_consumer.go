package consumer

import (
	"context"
	"database/sql"
	"log"

	orderv1 "event-market/api/gen/v1/order"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func StartPaymentConsumer(ctx context.Context, db *sql.DB, brokerAddress string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   "payment.events",
		GroupID: "order-service-payment-group",
	})
	defer reader.Close()

	log.Println("[ORDER-SERVICE] Payment consumer started, listening to 'payment.events'...")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[ORDER-SERVICE] Stopping payment consumer...")
				return
			}
			log.Printf("[ERROR] Failed to read payment event: %v", err)
			continue
		}

		var paymentEvent orderv1.PaymentProcessedEvent
		if err := proto.Unmarshal(msg.Value, &paymentEvent); err != nil {
			log.Printf("[ERROR] Failed to unmarshal PaymentProcessedEvent: %v", err)
			continue
		}

		newStatus := "PAID"
		if !paymentEvent.GetSuccess() {
			newStatus = "PAYMENT_FAILED"
		}
		
		query := `UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2`
		result, err := db.ExecContext(ctx, query, newStatus, paymentEvent.GetOrderId())
		if err != nil {
			log.Printf("[ERROR] Failed to update order status in DB: %v", err)
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			log.Printf("[WARN] Order with ID %s not found for status update", paymentEvent.GetOrderId())
			continue
		}

		log.Printf("🎉 [ORDER STATUS UPDATED] Order ID: %s -> Status: %s", paymentEvent.GetOrderId(), newStatus)
	}
}
