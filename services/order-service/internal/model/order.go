package model

import "time"

// данных о предмете
type OrderItemDTO struct {
	ItemID   string  `json:"item_id"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// представление данных о запросе на создание заказа
type CreateOrderRequest struct {
	UserID string         `json:"user_id"`
	Items  []OrderItemDTO `json:"items"`
}

// информация о заказе, которая хранится в базе данных
type Order struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	TotalAmount float64   `json:"total_amount"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// информация о заказе, которая возвращается клиенту после создания заказа
type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}
