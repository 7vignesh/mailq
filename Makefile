DB_URL ?= postgres://mailq:mailq@localhost:55432/mailq?sslmode=disable

.PHONY: db build test test-unit vet fmt

db:
	docker compose up -d --wait postgres

build:
	go build -o bin/ ./cmd/...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# Unit tests only: no database needed. DB-backed tests skip themselves.
test-unit:
	go test -race ./...

# Full suite, including Postgres-backed store and integration tests.
test: db
	MAILQ_TEST_DATABASE_URL="$(DB_URL)" go test -race -count=1 ./...
