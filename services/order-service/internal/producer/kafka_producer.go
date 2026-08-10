package producer

import (
	"context"
	orderv1 "event-market/api/gen/v1/order"
	"log"

	kafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type OrderProducer interface {
	PublishOrderCreated(ctx context.Context, event *orderv1.OrderCreatedEvent) error
	Close() error
}

type KafkaOrderProducer struct {
	write *kafka.Writer
}

func NewKafkaOrderProducer(brokers []string, topic string) *KafkaOrderProducer {
	return &KafkaOrderProducer{
		write: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (kp *KafkaOrderProducer) PublishOrderCreated(ctx context.Context, event *orderv1.OrderCreatedEvent) error {
	bytes, err := proto.Marshal(event)
	if err != nil {
		log.Default().Printf("Error marshaling event: %v", err)
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.GetOrderId()),
		Value: bytes,
	}

	return kp.write.WriteMessages(ctx, msg)
}

func (kp *KafkaOrderProducer) Close() error {
	return kp.write.Close()
}
