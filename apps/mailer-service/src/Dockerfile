# --- Stage 1: Builder (Βαρύ Image με C-Compilers) ---
FROM golang:1.24-bullseye AS builder

WORKDIR /app

# Αντιγραφή του κώδικα
COPY main.go .

# Αρχικοποίηση του go.mod και κατέβασμα των dependencies
# (Βάζουμε το confluent-kafka-go ρητά για να μην ψάχνει)
RUN go mod init mailer-service && \
    go mod tidy

# Compile του Binary
# Το CGO_ENABLED=1 είναι υποχρεωτικό για τη βιβλιοθήκη του Kafka (librdkafka)
RUN CGO_ENABLED=1 GOOS=linux go build -o mailer-service .


# --- Stage 2: Runtime (Ελαφρύ Image για το K8s) ---
# Χρησιμοποιούμε debian-slim αντί για alpine γιατί το CGO έχει glibc dependencies
FROM debian:bullseye-slim

# Εγκατάσταση πιστοποιητικών συστήματος (Απαραίτητο για το SSL του Kafka και το SMTP TLS)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Αντιγραφή ΜΟΝΟ του τελικού εκτελέσιμου από το Stage 1
COPY --from=builder /app/mailer-service .

# (Προαιρετικό αλλά καλό practice) Δημιουργούμε τον φάκελο που θα κάνει mount το K8s τα Kafka Certs
RUN mkdir -p /app/certs

# Εκτέλεση του microservice
CMD ["./mailer-service"]