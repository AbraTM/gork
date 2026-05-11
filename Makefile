.PHONY: check build lint test benchmark run dev setup

all: check

check: build lint test
	@echo "all checks passed"

build:
	@echo "-> building..."
	@go build ./...
	@echo "build passed"
	@echo ""

lint:
	@echo "-> linting..."
	@golangci-lint run ./...
	@echo "lint passed"
	@echo ""

test:
	@echo "-> testing..."
	@go test -race ./...
	@echo "tests passed"
	@echo "" 

rabbitmq:
	@docker start gork-rabbitmq 2>/dev/null || \
		docker run -d --name gork-rabbitmq \
		-p 5672:5672 \
		-p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq collect_statistics_interval 500" \
		rabbitmq:3-management
	@echo ""
	@echo "RabbitMQ running on http://127.0.0.1:5672"
	@echo "RabbitMQ management UI on http://127.0.0.1:15672 (guest/guest)"

benchmark: rabbitmq
	@echo ""
	@echo "-> running benchmark..."
	@go run cmd/benchmark/main.go

dev: rabbitmq
	@echo ""
	@echo "-> starting engine..."
	@go run cmd/gork/*.go run

setup:
	@cp scripts/pre-push .git/hooks/pre-push
	@chmod +x .git/hooks/pre-push
	@echo "git hooks installed"

run:
	@go run cmd/gork/*.go run

