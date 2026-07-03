package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	// For metrics and monitoring
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	emailsSentTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "go_emails_processed_total",
			Help: "Total number of notification emails processed from Kafka",
		},
		[]string{"status"}, //"success" or "error"
	)
)

func init() {
	// --- 2. REGISTRATION FOR METRIC PROMETHEUS --
	prometheus.MustRegister(emailsSentTotal)
}

// EnrichedMessage represents the data model consumed from the Kafka topic.
// It contains account and customer information used to send balance notification emails.
type EnrichedMessage struct {
	AccountID    string  `json:"account_id"`
	Balance      float64 `json:"balance"`
	CustomerName string  `json:"customer_name"`
	Email        string  `json:"email"`
	ProcessedAt  string  `json:"processed_at"`
}

// sendEmail establishes a STARTTLS connection to the SMTP server and sends
// a balance notification email to the customer.
func sendEmail(msg EnrichedMessage, smtpHost string, smtpPort string, smtpUser string, smtpPass string) error {

	// Build email headers (From, To, Subject, MIME)
	headers := fmt.Sprintf("From: GBank Notifications <noreply@banking.local>\r\n")
	headers += fmt.Sprintf("To: %s <%s>\r\n", msg.CustomerName, msg.Email)
	headers += fmt.Sprintf("Subject: Balance Inquiry - %s\r\n", msg.AccountID)
	headers += "MIME-version: 1.0;\r\n"
	headers += "Content-Type: text/plain; charset=\"UTF-8\"\r\n"
	headers += "\r\n" // blank line separates headers from body

	// Build email body
	body := fmt.Sprintf("Dear %s,\n\nYour new balance is: %.2f €.", msg.CustomerName, msg.Balance)
	message := []byte(headers + body)

	smtpAddr := fmt.Sprintf("%s:%s", smtpHost, smtpPort)

	// Step 1: Open a plain TCP connection to the SMTP server
	client, err := smtp.Dial(smtpAddr)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer client.Close()

	// Step 2: Upgrade the connection to TLS via STARTTLS.
	// InsecureSkipVerify is acceptable for thesis/dev environments with self-signed certificates.

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         smtpHost,
	}
	if err = client.StartTLS(tlsConfig); err != nil {
		return fmt.Errorf("STARTTLS failed: %w", err)
	}

	// Step 3: Authenticate with the SMTP server using PLAIN auth (only if credentials are provided)
	if smtpUser != "" && smtpPass != "" {
		auth := smtp.PlainAuth("", smtpUser, smtpPass, smtpHost)
		if err = client.Auth(auth); err != nil {
			return fmt.Errorf("authentication failed: %w", err)
		}
	}

	// Step 4: Set envelope sender and recipient
	if err = client.Mail("noreply@banking.local"); err != nil {
		return err
	}
	if err = client.Rcpt(msg.Email); err != nil {
		return err
	}

	// Step 5: Write the email content
	w, err := client.Data()
	if err != nil {
		return err
	}
	if _, err = w.Write(message); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}

	return client.Quit()
}
func main() {

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Println("📈 Prometheus metrics server listening on :8081/metrics")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			fmt.Printf("Metrics server failed to start: %v\n", err)
		}
	}()

	// --- Processing delay (used to simulate heavy workload and trigger KEDA scaling) ----
	delayStr := os.Getenv("PROCESSING_DELAY_MS")
	delayMs, err := strconv.Atoi(delayStr)
	if err != nil || delayMs <= 0 {
		delayMs = 100 // default: 100ms
	}

	// --- Kafka configuration ---
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "my-cluster-kafka-bootstrap:9093"
	}

	// --- SMTP configuration ---
	smtpHost := os.Getenv("SMTP_HOST")
	if smtpHost == "" {
		smtpHost = "mailpit-service" // Kubernetes service name
	}
	smtpPort := os.Getenv("SMTP_PORT")
	if smtpPort == "" {
		smtpPort = "1025"
	}
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASSWORD")

	// --- Initialize Kafka Consumer ---
	// SSL is required for mTLS communication with the Strimzi Kafka cluster.
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   kafkaBrokers,
		"group.id":                            "go-mailer-group",
		"auto.offset.reset":                   "earliest", // ensure zero data loss on restart
		"enable.auto.commit":                  false,      // manual commit for better control
		"security.protocol":                   "SSL",
		"ssl.ca.location":                     "/app/certs/ca.crt",
		"ssl.certificate.location":            "/app/certs/user.crt",
		"ssl.key.location":                    "/app/certs/user.key",
		"enable.ssl.certificate.verification": false,
		"session.timeout.ms":                  45000,
		"max.poll.interval.ms":                300000,
	})
	if err != nil {
		fmt.Printf(" Failed to create Kafka consumer: %v\n", err)
		os.Exit(1)
	}

	consumer.SubscribeTopics([]string{"profile-updates"}, nil)
	fmt.Printf(" Mailer Service started! (Kafka: %s, SMTP: %s:%s)\n", kafkaBrokers, smtpHost, smtpPort)

	// --- Graceful shutdown: listen for SIGINT / SIGTERM (KEDA sends SIGTERM on scale-down) ---
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	run := true
	// --- Start Kafka polling loop in a background goroutine ---
	wg.Add(1)
	go func() {
		defer wg.Done()

		for run {
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var data EnrichedMessage
				if err := json.Unmarshal(e.Value, &data); err != nil {
					fmt.Printf(" Failed to deserialize message: %v\n", err)
					consumer.CommitMessage(e)
					continue
				}

				time.Sleep(time.Duration(delayMs) * time.Millisecond)
				if err := sendEmail(data, smtpHost, smtpPort, smtpUser, smtpPass); err != nil {
					fmt.Printf(" Failed to send email to %s: %v\n", data.Email, err)
					emailsSentTotal.WithLabelValues("error").Inc()

					consumer.CommitMessage(e)
				} else {
					fmt.Printf("[%s]  Email sent to %s (Account: %s)\n",
						time.Now().Format("15:04:05"), data.Email, data.AccountID)
					emailsSentTotal.WithLabelValues("success").Inc()

					consumer.CommitMessage(e)
				}
			case kafka.Error:
				fmt.Printf("  Kafka error: %v\n", e)
			}
		}
	}()
	// --- Block until shutdown signal is received ---
	<-sigchan
	fmt.Println("\n  Shutdown signal received (SIGTERM). Closing consumer...")

	run = false
	wg.Wait()
	consumer.Close()
	fmt.Println(" Mailer Service shut down safely.")
}
