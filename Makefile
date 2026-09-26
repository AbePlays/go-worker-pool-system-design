.PHONY: run test vet build fmt docker-build docker-run migrate-up migrate-down compose-up compose-down reset

run:
	go run .

test:
	go test -p 1 ./... -race

vet:
	go vet ./...

build:
	go build ./...

fmt:
	test -z "$$(gofmt -l .)"

docker-build:
	docker build -t workerpool .

docker-run:
	docker run --rm -p 8080:8080 -e PORT=8080 -e WORKERS=8 -e JOB_TIMEOUT=30 workerpool

migrate-up:
	migrate -path db/migrations -database postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable up

migrate-down:
	migrate -path db/migrations -database postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable down 1

compose-up:
	docker compose up --build

compose-down:
	docker compose down

reset:
	docker compose down -v
