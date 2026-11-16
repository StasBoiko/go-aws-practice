# --- Stage 1: Builder ---
FROM golang:1.27.3-alpine AS builder

# Устанавливаем ca-certificates, чтобы скопировать их в scratch-образ
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Сначала копируем go.mod и go.sum для кэширования слоев зависимостей
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Собираем статический бинарник без CGO
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/service ./cmd/notifier

# --- Stage 2: Runtime (минимальный образ ~15 MB) ---
FROM scratch

# Копируем корневые SSL-сертификаты для HTTPS-запросов к AWS API
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/service /service

EXPOSE 8080

ENTRYPOINT ["/service"]