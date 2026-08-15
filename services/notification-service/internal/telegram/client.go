package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	orderv1 "event-market/api/gen/v1/order"
)

type TelegramPayload struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

type EditMessagePayload struct {
	ChatID    string `json:"chat_id"`
	MessageID int    `json:"message_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

type TelegramResponse struct {
	Ok     bool `json:"ok"`
	Result struct {
		MessageID int `json:"message_id"`
	} `json:"result"`
}

type OrderState struct {
	MessageID int
	Status    string // "CREATED", "PAID", "FAILED"
	Items     []*orderv1.OrderItem
}

type Client struct {
	httpClient *http.Client
	botToken   string
	chatID     string
	states     sync.Map // order_id (string) -> *OrderState
}

func NewClient(token, chatID string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		botToken:   token,
		chatID:     chatID,
	}
}

func (c *Client) SendOrderCard(event *orderv1.OrderCreatedEvent) error {
	orderID := event.GetOrderId()

	// 1. Проверяем, не обработалась ли оплата РАНЬШЕ создания заказа
	if val, ok := c.states.Load(orderID); ok {
		state := val.(*OrderState)
		if state.Status == "PAID" || state.Status == "FAILED" {
			log.Printf("⚠️ [SKIP] OrderCreated for %s ignored: order is already in %s status", orderID, state.Status)
			return nil
		}
	}

	text := c.renderCard("CREATED", orderID, "", event.GetItems())

	msgID, err := c.sendMessage(text)
	if err != nil {
		return err
	}

	// 2. Запоминаем ID сообщения и статус
	c.states.Store(orderID, &OrderState{
		MessageID: msgID,
		Status:    "CREATED",
		Items:     event.GetItems(),
	})

	log.Printf("📌 [STORED] Message ID %d stored for Order ID: %s", msgID, orderID)
	return nil
}

func (c *Client) UpdateOrderCard(event *orderv1.PaymentProcessedEvent) error {
	orderID := event.GetOrderId()
	var state *OrderState
	found := false

	// Ожидаем до 3 секунд на случай, если HandleOrderCreated сейчас выполняет HTTP-запрос
	for i := 0; i < 10; i++ {
		if val, ok := c.states.Load(orderID); ok {
			state = val.(*OrderState)
			found = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	status := "PAID"
	if !event.GetSuccess() {
		status = "FAILED"
	}

	if found && state.Status == "CREATED" {
		// Стандартный порядок: обновляем созданную карточку
		text := c.renderCard(status, orderID, event.GetPaymentId(), state.Items)
		if err := c.editMessage(state.MessageID, text); err != nil {
			log.Printf("[WARN] Failed to edit message %d: %v", state.MessageID, err)
			return err
		}

		state.Status = status
		log.Printf("✨ [UPDATED] Live card updated for Order: %s", orderID)
		return nil
	}

	// Аномальный порядок (Out-of-Order): событие оплаты пришло раньше создания
	text := c.renderCard(status, orderID, event.GetPaymentId(), nil)
	msgID, err := c.sendMessage(text)
	if err != nil {
		return err
	}

	// Фиксируем, что статус УЖЕ terminal (PAID/FAILED), чтобы запоздавший SendOrderCard ничего не отправлял
	c.states.Store(orderID, &OrderState{
		MessageID: msgID,
		Status:    status,
	})

	log.Printf("⚠️ [OUT OF ORDER] Payment processed before creation card for Order: %s", orderID)
	return nil
}

func (c *Client) sendMessage(text string) (int, error) {
	payload := TelegramPayload{ChatID: c.chatID, Text: text, ParseMode: "HTML"}
	body, _ := json.Marshal(payload)

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.botToken)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("telegram status: %s", resp.Status)
	}

	var tgResp TelegramResponse
	if err := json.NewDecoder(resp.Body).Decode(&tgResp); err != nil {
		return 0, err
	}
	return tgResp.Result.MessageID, nil
}

func (c *Client) editMessage(msgID int, text string) error {
	payload := EditMessagePayload{ChatID: c.chatID, MessageID: msgID, Text: text, ParseMode: "HTML"}
	body, _ := json.Marshal(payload)

	url := fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", c.botToken)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram edit status: %s", resp.Status)
	}
	return nil
}

func (c *Client) renderCard(status, orderID, paymentID string, items []*orderv1.OrderItem) string {
	var progressBar, statusBadge string

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
	var total float64
	for _, item := range items {
		sum := float64(item.GetQuantity()) * item.GetPrice()
		total += sum
		itemsList += fmt.Sprintf("  ▫️ <code>%s</code> × %d шт. — <b>%.2f ₽</b>\n", item.GetItemId(), item.GetQuantity(), sum)
	}

	card := fmt.Sprintf(
		"🛒 <b>EVENT MARKET</b> | Заказ <code>#%s</code>\n"+
			"───────────────────────────\n"+
			"Статус: %s\n\n"+
			"<b>Прогресс заказа:</b>\n%s\n\n"+
			"📦 <b>Состав заказа:</b>\n%s\n"+
			"💵 <b>Сумма:</b> <code>%.2f ₽</code>\n",
		shortID, statusBadge, progressBar, itemsList, total,
	)

	if paymentID != "" {
		card += fmt.Sprintf("🧾 <b>Чек оплаты:</b> <code>%s</code>\n", paymentID)
	}
	return card + "───────────────────────────"
}
