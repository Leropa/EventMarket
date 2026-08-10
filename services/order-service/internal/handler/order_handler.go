package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	orderv1 "event-market/api/gen/v1/order"
	"event-market/services/order-service/internal/model"
	"event-market/services/order-service/internal/producer"
	"event-market/services/order-service/internal/repository"

	"github.com/google/uuid"
)

type OrderHandler struct {
	repo     repository.OrderRepository
	producer producer.OrderProducer
}

func NewOrderHandler(repo repository.OrderRepository, producer producer.OrderProducer) *OrderHandler {
	return &OrderHandler{
		repo:     repo,
		producer: producer,
	}
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req model.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[ERROR] Failed to decode JSON body: %v", err)
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	totalAmount := 0.0
	for _, item := range req.Items {
		totalAmount += float64(item.Quantity) * item.Price
	}

	orderID := uuid.New().String()

	order := &model.Order{
		ID:          orderID,
		UserID:      req.UserID,
		TotalAmount: totalAmount,
		Status:      "PENDING",
		CreatedAt:   time.Now(),
	}

	err := h.repo.Save(r.Context(), order)
	if err != nil {
		log.Printf("[ERROR] Postgres Save failed: %v", err)
		http.Error(w, "Failed to create order in database", http.StatusInternalServerError)
		return
	}

	protoItems := make([]*orderv1.OrderItem, 0, len(req.Items))
	for _, item := range req.Items {
		orderItem := &orderv1.OrderItem{
			ItemId:   item.ItemID,
			Quantity: int32(item.Quantity),
			Price:    item.Price,
		}
		protoItems = append(protoItems, orderItem)
	}

	event := &orderv1.OrderCreatedEvent{
		OrderId:     orderID,
		UserId:      req.UserID,
		TotalAmount: totalAmount,
		Items:       protoItems,
		CreatedAt:   time.Now().Unix(),
	}

	err = h.producer.PublishOrderCreated(r.Context(), event)
	if err != nil {
		log.Printf("[ERROR] Kafka Publish failed: %v", err)
		http.Error(w, "Failed to produce order created event", http.StatusInternalServerError)
		return
	}

	response := model.CreateOrderResponse{
		OrderID: orderID,
		Status:  "PENDING",
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[ERROR] Failed to encode response: %v", err)
	}
}
