FROM golang:1.27-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /workerpool .
RUN CGO_ENABLED=0 go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.20.1

FROM alpine:3.20

WORKDIR /app

COPY --from=build /workerpool /app/workerpool
COPY --from=build /go/bin/migrate /app/migrate
COPY db/migrations /app/db/migrations
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

EXPOSE 8080

CMD ["/app/entrypoint.sh"]
