FROM golang:1.27-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /workerpool .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /workerpool /app/workerpool
EXPOSE 8080
CMD ["/app/workerpool"]
