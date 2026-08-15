FROM golang:1.26 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY api/ ./api/
COPY services/notification-service/ ./services/notification-service/

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/notification-service ./services/notification-service/cmd/notifier/main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/notification-service .

CMD ["./notification-service"]
