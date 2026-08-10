FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY api/ ./api/
COPY services/order-service/ ./services/order-service/

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/order-service ./services/order-service/cmd/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/order-service .

EXPOSE 8081

CMD ["./order-service"]