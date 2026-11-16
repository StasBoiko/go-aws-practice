.PHONY: local-up local-down run-notifier test build-docker

# Поднять локальную инфраструктуру AWS (LocalStack)
local-up:
	docker compose up -d localstack

local-down:
	docker compose down

# Запуск сервиса отправки писем с конфигом для local
run-notifier:
	set -a && source configs/local.env && set +a && go run ./cmd/notifier

# Проверка кода и тесты (запускается в CI)
test:
	go vet ./...
	go test -v -race ./...

# Сборка продакшен Docker-образа для нотификатора
build-docker:
	docker build -t go-aws-notifier:latest .