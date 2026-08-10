package repository

import (
	"context"
	"database/sql"
	"event-market/services/order-service/internal/model"
	"log"
)

type OrderRepository interface {
	Save(ctx context.Context, order *model.Order) error
}

type PostgresOrderRepository struct {
	db *sql.DB
}

func NewPostgresOrderRepository(db *sql.DB) *PostgresOrderRepository {
	return &PostgresOrderRepository{db: db}
}

func (pr *PostgresOrderRepository) Save(ctx context.Context, order *model.Order) error {
	query := `
		INSERT INTO orders (id, user_id, total_amount, status, created_at) 	
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := pr.db.ExecContext(
		ctx, query, order.ID, order.UserID,
		order.TotalAmount, order.Status, order.CreatedAt)

	if err != nil {
		log.Default().Printf("Error saving order: %v", err)
	}

	return err
}
