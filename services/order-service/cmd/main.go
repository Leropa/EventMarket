package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"

	"event-market/services/order-service/internal/handler"
	"event-market/services/order-service/internal/producer"
	"event-market/services/order-service/internal/repository"
)

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("FATAL: environment variable %s is required but not set", key)
	}
	return value
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

func main() {
	dbURL := mustGetEnv("DATABASE_URL")
	kafkaBrokers := mustGetEnv("KAFKA_BROKERS")

	connStr := dbURL
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Successfully connected to PostgreSQL")

	brokers := []string{kafkaBrokers}
	topic := "order.events"
	kafkaProducer := producer.NewKafkaOrderProducer(brokers, topic)
	defer kafkaProducer.Close()

	orderRepo := repository.NewPostgresOrderRepository(db)
	orderHandler := handler.NewOrderHandler(orderRepo, kafkaProducer)

	http.HandleFunc("/api/v1/orders", corsMiddleware(orderHandler.CreateOrder))

	log.Println("Order Service is running on port :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
