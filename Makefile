.PHONY: run test vet build fmt docker-build docker-run

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
