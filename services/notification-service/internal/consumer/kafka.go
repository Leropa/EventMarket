package consumer

import (
	"context"
	"log"

	orderv1 "event-market/api/gen/v1/order"
	"event-market/services/notification-service/internal/telegram"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type EventConsumer struct {
	orderReader   *kafka.Reader
	paymentReader *kafka.Reader
	tgClient      *telegram.Client
}

func NewEventConsumer(brokers string, tgClient *telegram.Client) *EventConsumer {
	return &EventConsumer{
		orderReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{brokers},
			Topic:    "order.events",
			GroupID:  "notification-order-group",
			MinBytes: 10 * 1024,
			MaxBytes: 10 * 1024 * 1024,
		}),
		paymentReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{brokers},
			Topic:    "payment.events",
			GroupID:  "notification-payment-group",
			MinBytes: 10 * 1024,
			MaxBytes: 10 * 1024 * 1024,
		}),
		tgClient: tgClient,
	}
}

func (c *EventConsumer) Start(ctx context.Context) {
	go c.consumeOrders(ctx)
	go c.consumePayments(ctx)
}

func (c *EventConsumer) Close() {
	_ = c.orderReader.Close()
	_ = c.paymentReader.Close()
}

func (c *EventConsumer) consumeOrders(ctx context.Context) {
	for {
		m, err := c.orderReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled, exit gracefully
			}
			log.Printf("[ERROR] Read order message: %v", err)
			continue
		}

		var event orderv1.OrderCreatedEvent
		if err := proto.Unmarshal(m.Value, &event); err != nil {
			log.Printf("[ERROR] Unmarshal OrderCreatedEvent: %v", err)
			continue
		}

		if err := c.tgClient.SendOrderCard(&event); err != nil {
			log.Printf("[ERROR] Process OrderCreated: %v", err)
		}
	}
}

func (c *EventConsumer) consumePayments(ctx context.Context) {
	for {
		m, err := c.paymentReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[ERROR] Read payment message: %v", err)
			continue
		}

		var event orderv1.PaymentProcessedEvent
		if err := proto.Unmarshal(m.Value, &event); err != nil {
			log.Printf("[ERROR] Unmarshal PaymentProcessedEvent: %v", err)
			continue
		}

		if err := c.tgClient.UpdateOrderCard(&event); err != nil {
			log.Printf("[ERROR] Process PaymentProcessed: %v", err)
		}
	}
}
