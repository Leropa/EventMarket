package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"event-market/services/notification-service/internal/config"
	"event-market/services/notification-service/internal/consumer"
	"event-market/services/notification-service/internal/telegram"
)

func main() {
	cfg := config.Load()

	tgClient := telegram.NewClient(cfg.TelegramBotToken, cfg.TelegramChatID)
	eventConsumer := consumer.NewEventConsumer(cfg.KafkaBrokers, tgClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("🔔 [NOTIFICATION-SERVICE] Service started...")
	eventConsumer.Start(ctx)

	// Graceful Shutdown implementation
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("🛑 [NOTIFICATION-SERVICE] Shutting down gracefully...")

	cancel()
	eventConsumer.Close()
	log.Println("👋 [NOTIFICATION-SERVICE] Service stopped")
}
