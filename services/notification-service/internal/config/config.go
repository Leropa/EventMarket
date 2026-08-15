package config

import (
	"log"
	"os"
)

type Config struct {
	TelegramBotToken string
	TelegramChatID   string
	KafkaBrokers     string
}

func Load() *Config {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	brokers := os.Getenv("KAFKA_BROKERS")

	if token == "" || chatID == "" || brokers == "" {
		log.Fatal("FATAL: Required environment variables (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, KAFKA_BROKERS) are not set")
	}

	return &Config{
		TelegramBotToken: token,
		TelegramChatID:   chatID,
		KafkaBrokers:     brokers,
	}
}
