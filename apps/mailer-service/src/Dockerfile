FROM golang:1.24-bullseye AS builder

WORKDIR /app

COPY main.go .

# Beggining of the build process
RUN go mod init mailer-service && \
    go mod tidy

# Compile of Binary
# CGO_ENABLED=1 for Kafka (librdkafka)
RUN CGO_ENABLED=1 GOOS=linux go build -o mailer-service .

FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/mailer-service .

RUN mkdir -p /app/certs

CMD ["./mailer-service"]