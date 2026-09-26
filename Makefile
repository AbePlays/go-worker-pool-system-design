.PHONY: run test vet build fmt docker-build docker-run migrate-up migrate-down

run:
	go run .

test:
	go test ./... -race

vet:
	go vet ./...

build:
	go build ./...

fmt:
	gofmt -l .

docker-build:
	docker build -t workerpool .

docker-run:
	docker run --rm -p 8080:8080 -e PORT=8080 -e WORKERS=8 -e JOB_TIMEOUT=30 workerpool

migrate-up:
	migrate -path db/migrations -database postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable up

migrate-down:
	migrate -path db/migrations -database postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable down 1
