.PHONY: run test vet

run:
	go run .

test:
	go test ./... -race

vet:
	go vet ./...
