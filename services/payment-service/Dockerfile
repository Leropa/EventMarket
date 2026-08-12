# Этап сборки
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Копируем файлы зависимостей из корня проекта
COPY go.mod go.sum ./
RUN go mod download

# Копируем все исходники монорепозитория
COPY . .

# Собираем бинарный файл сервиса
RUN CGO_ENABLED=0 GOOS=linux go build -o /payment-service ./services/payment-service/cmd/main.go

# Финальный легкий образ
FROM alpine:latest

WORKDIR /app

COPY --from=builder /payment-service .

CMD ["./payment-service"]
