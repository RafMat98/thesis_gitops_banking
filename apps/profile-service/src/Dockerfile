# --- Stage 1: Build Image (All the tools needed to compile the Go code) ---
FROM golang:1.24-bullseye AS builder

# 1. Install dependencies for Kafka (librdkafka) and CGo (gcc, libc-dev)
RUN apt-get update && apt-get install -y \
    pkg-config librdkafka-dev gcc libc6-dev make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Copy only the code (since you don't want to run commands locally)
COPY main.go .

# 3. Automatic creation of go.mod and downloading of libraries inside the Docker
RUN go mod init profile-service && go mod tidy

# 4. Compile του Binary (με CGO_ENABLED=1 for Kafka)
RUN CGO_ENABLED=1 GOOS=linux go build -o profile-service .

# --- stage 2: Runtime Image (Only the compiled binary and the necessary libraries) ---
FROM debian:bullseye-slim

# Install only the necessary library for execution
RUN apt-get update && apt-get install -y \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/profile-service .

# Εκτέλεση
CMD ["./profile-service"]