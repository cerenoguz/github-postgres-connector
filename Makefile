.PHONY: build test test-unit lint up down sync

build: ## Build the binary into ./bin
	go build -o bin/connector ./cmd/connector

test: ## All tests; the integration tests need Docker
	go test -race -count=1 ./...

test-unit: ## Only the tests that need nothing but Go
	go test -race -count=1 -short ./...

lint: ## Fails if any file is unformatted or a check reports a problem
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "not gofmt-ed:"; echo "$$unformatted"; exit 1; fi
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

up: ## Start a local PostgreSQL
	docker compose up -d --wait

down:
	docker compose down

sync: build ## Run a sync with ./config.yaml
	./bin/connector sync --config config.yaml
