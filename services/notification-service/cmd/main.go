package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	orderv1 "event-market/api/gen/v1/order"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// TelegramPayload — структура для отправки нового сообщения
type TelegramPayload struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

// EditMessagePayload — структура для обновления существующего сообщения
type EditMessagePayload struct {
	ChatID    string `json:"chat_id"`
	MessageID int    `json:"message_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

// TelegramResponse — ответ от Telegram API с данными об отправленном сообщении
type TelegramResponse struct {
	Ok     bool `json:"ok"`
	Result struct {
		MessageID int `json:"message_id"`
	} `json:"result"`
}

// OrderData — хранит ID сообщения и состав заказа в памяти
type OrderData struct {
	MessageID int
	Items     []*orderv1.OrderItem
}

type NotificationService struct {
	httpClient *http.Client
	botToken   string
	chatID     string
	messages   sync.Map // Map[order_id string]OrderData
}

func NewNotificationService(token, chatID string) *NotificationService {
	return &NotificationService{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		botToken: token,
		chatID:   chatID,
	}
}

// Отправка нового сообщения в Telegram
func (s *NotificationService) sendTelegramMessage(text string) (int, error) {
	payload := TelegramPayload{
		ChatID:    s.chatID,
		Text:      text,
		ParseMode: "HTML",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payload: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.botToken)
	resp, err := s.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("telegram sendMessage status: %s", resp.Status)
	}

	var tgResp TelegramResponse
	if err := json.NewDecoder(resp.Body).Decode(&tgResp); err != nil {
		return 0, fmt.Errorf("failed to decode telegram response: %w", err)
	}

	return tgResp.Result.MessageID, nil
}

// Редактирование существующего сообщения
func (s *NotificationService) editTelegramMessage(messageID int, text string) error {
	payload := EditMessagePayload{
		ChatID:    s.chatID,
		MessageID: messageID,
		Text:      text,
		ParseMode: "HTML",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal edit payload: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", s.botToken)
	resp, err := s.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram editMessage status: %s", resp.Status)
	}

	return nil
}

// Отрисовка UI-карточки заказа в стиле маркетплейсов
func (s *NotificationService) renderOrderCard(status string, orderID string, paymentID string, items []*orderv1.OrderItem) string {
	var progressBar string
	var statusBadge string

	switch status {
	case "CREATED":
		progressBar = "🟢 <b>Создан</b> ── 🟡 <b>Оплата</b> ── ⚪ Сборка ── ⚪ В пути"
		statusBadge = "⏳ <b>ОЖИДАЕТ ОПЛАТЫ</b>"
	case "PAID":
		progressBar = "🟢 Создан ── 🟢 <b>Оплачен</b> ── 🚚 <b>Передан в доставку</b>"
		statusBadge = "✅ <b>ОПЛАЧЕНО</b>"
	case "FAILED":
		progressBar = "🟢 Создан ── 🔴 <b>Ошибка оплаты</b> ── 🛑 Отменен"
		statusBadge = "❌ <b>ОШИБКА ОПЛАТЫ</b>"
	}

	shortID := orderID
	if len(orderID) > 8 {
		shortID = orderID[:8]
	}

	var itemsList string
	var totalAmount float64
	for _, item := range items {
		itemTotal := float64(item.GetQuantity()) * item.GetPrice()
		totalAmount += itemTotal
		itemsList += fmt.Sprintf("  ▫️ <code>%s</code> × %d шт. — <b>%.2f ₽</b>\n",
			item.GetItemId(), item.GetQuantity(), itemTotal)
	}

	if itemsList == "" {
		itemsList = "  ▫️ <i>Состав заказа уточняется</i>\n"
	}

	card := fmt.Sprintf(
		"🛒 <b>EVENT MARKET</b> | Заказ <code>#%s</code>\n"+
			"───────────────────────────\n"+
			"Статус: %s\n\n"+
			"<b>Прогресс заказа:</b>\n"+
			"%s\n\n"+
			"📦 <b>Состав заказа:</b>\n%s\n"+
			"💵 <b>Сумма:</b> <code>%.2f ₽</code>\n",
		shortID, statusBadge, progressBar, itemsList, totalAmount,
	)

	if paymentID != "" {
		card += fmt.Sprintf("🧾 <b>Чек оплаты:</b> <code>%s</code>\n", paymentID)
	}

	card += "───────────────────────────"
	return card
}

// 1. Обработка события order.events
func (s *NotificationService) HandleOrderCreated(event *orderv1.OrderCreatedEvent) error {
	text := s.renderOrderCard("CREATED", event.GetOrderId(), "", event.GetItems())

	msgID, err := s.sendTelegramMessage(text)
	if err != nil {
		return err
	}

	// Сохраняем ID сообщения и список товаров
	s.messages.Store(event.GetOrderId(), OrderData{
		MessageID: msgID,
		Items:     event.GetItems(),
	})

	log.Printf("📌 [STORED] Message ID %d stored for Order ID: %s", msgID, event.GetOrderId())
	return nil
}

// 2. Обработка события payment.events
func (s *NotificationService) HandlePaymentProcessed(event *orderv1.PaymentProcessedEvent) error {
	var savedData OrderData
	found := false

	// Retry loop: ждем до 3 секунд, пока HandleOrderCreated успеет записать данные в sync.Map
	for i := 0; i < 10; i++ {
		if val, ok := s.messages.Load(event.GetOrderId()); ok {
			savedData = val.(OrderData)
			found = true
			s.messages.Delete(event.GetOrderId()) // очищаем память после успешного получения
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	status := "PAID"
	if !event.GetSuccess() {
		status = "FAILED"
	}

	text := s.renderOrderCard(status, event.GetOrderId(), event.GetPaymentId(), savedData.Items)

	if found {
		// Обновляем текст существующего сообщения в чате
		if err := s.editTelegramMessage(savedData.MessageID, text); err != nil {
			log.Printf("[WARN] Failed to edit message %d: %v. Sending new message.", savedData.MessageID, err)
			_, err = s.sendTelegramMessage(text)
			return err
		}
		log.Printf("✨ [UPDATED] Live card updated for Order: %s", event.GetOrderId())
		return nil
	}

	// Fallback: если сообщение не нашлось в памяти, отправляем новое
	log.Printf("[WARN] Creation message not found for Order: %s. Sending standalone alert.", event.GetOrderId())
	_, err := s.sendTelegramMessage(text)
	return err
}

func main() {
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	if botToken == "" || chatID == "" || kafkaBrokers == "" {
		log.Fatalf("FATAL: Environment variables (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, KAFKA_BROKERS) are not set")
	}

	notifier := NewNotificationService(botToken, chatID)

	// Читатель 1: order.events
	orderReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "order.events",
		GroupID:  "notification-order-group",
		MinBytes: 10 * 1024,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer orderReader.Close()

	// Читатель 2: payment.events
	paymentReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    "payment.events",
		GroupID:  "notification-payment-group",
		MinBytes: 10 * 1024,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer paymentReader.Close()

	log.Println("🔔 [NOTIFICATION-SERVICE] Started listening to 'order.events' and 'payment.events'...")

	// Горутина для обработки создания заказов
	go func() {
		for {
			m, err := orderReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("[ERROR] Failed to read order message: %v", err)
				continue
			}

			var event orderv1.OrderCreatedEvent
			if err := proto.Unmarshal(m.Value, &event); err != nil {
				log.Printf("[ERROR] Failed to unmarshal OrderCreatedEvent: %v", err)
				continue
			}

			if err := notifier.HandleOrderCreated(&event); err != nil {
				log.Printf("[ERROR] Failed to process order created: %v", err)
			} else {
				log.Printf("📝 [ORDER CREATED] Notification sent for Order: %s", event.GetOrderId())
			}
		}
	}()

	// Основной поток обрабатывает события оплаты
	for {
		m, err := paymentReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[ERROR] Failed to read payment message: %v", err)
			continue
		}

		var event orderv1.PaymentProcessedEvent
		if err := proto.Unmarshal(m.Value, &event); err != nil {
			log.Printf("[ERROR] Failed to unmarshal PaymentProcessedEvent: %v", err)
			continue
		}

		if err := notifier.HandlePaymentProcessed(&event); err != nil {
			log.Printf("[ERROR] Failed to process payment: %v", err)
		} else {
			log.Printf("📲 [PAYMENT PROCESSED] Final status updated for Order: %s", event.GetOrderId())
		}
	}
}
