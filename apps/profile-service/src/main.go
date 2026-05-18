package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os" // For environment variables
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/redis/go-redis/v9"
)

type BalanceResponse struct {
	AccountID string  `json:"account_id"`
	Balance   float64 `json:"balance"`
}

type EnrichedResponse struct {
	AccountID    string  `json:"account_id"`
	Balance      float64 `json:"balance"`
	CustomerName string  `json:"customer_name"`
	Email        string  `json:"email"`
	ProcessedAt  string  `json:"processed_at"`
}

var ctx = context.Background()

func main() {
	// --- Reading settings from Environment ----
	redisAddr := os.Getenv("REDIS_ADDR")
	redisPass := os.Getenv("REDIS_PASSWORD")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	// 1. Connection to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
		DB:       0,
	})
	// 2. Kafka Consumer (mTLS)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   kafkaBrokers,
		"group.id":                            "go-microservice-group",
		"security.protocol":                   "SSL",
		"ssl.ca.location":                     "/app/certs/ca.crt",
		"ssl.certificate.location":            "/app/certs/user.crt",
		"ssl.key.location":                    "/app/certs/user.key",
		"auto.offset.reset":                   "earliest",
		"enable.ssl.certificate.verification": false,
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %v\n", err)
		os.Exit(1)
	}
	// 3. Kafka Producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                   kafkaBrokers,
		"security.protocol":                   "SSL",
		"ssl.ca.location":                     "/app/certs/ca.crt",
		"ssl.certificate.location":            "/app/certs/user.crt",
		"ssl.key.location":                    "/app/certs/user.key",
		"enable.ssl.certificate.verification": false,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		os.Exit(1)
	}

	consumer.SubscribeTopics([]string{"balance-responses"}, nil)

	fmt.Printf("Profile Service started (Redis: %s, Kafka: %s)\n", redisAddr, kafkaBrokers)

	targetTopic := "profile-updates"

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			rawStr := string(msg.Value)
			fmt.Printf("Raw received: %s\n", rawStr)

			// 1. Split strings to char '+'
			parts := strings.Split(rawStr, "+")
			if len(parts) < 2 {
				fmt.Printf("Invalid message format: %s\n", rawStr)
				continue
			}

			accID := parts[0]
			balStr := parts[1]

			// 2. Transform balance string to float64
			balance, err := strconv.ParseFloat(balStr, 64)
			if err != nil {
				fmt.Printf("Error parsing balance: %v\n", err)
				continue
			}

			// 3. Create the struct for Redis lookup
			raw := BalanceResponse{
				AccountID: accID,
				Balance:   balance,
			}

			// 4. Enriching with Redis data
			customerName, _ := rdb.Get(ctx, raw.AccountID+":name").Result()
			customerEmail, _ := rdb.Get(ctx, raw.AccountID+":email").Result()

			enriched := EnrichedResponse{
				AccountID:    raw.AccountID,
				Balance:      raw.Balance,
				CustomerName: customerName,
				Email:        customerEmail,
				ProcessedAt:  time.Now().Format(time.RFC3339),
			}

			payload, _ := json.Marshal(enriched)

			// Producing the enriched message
			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &targetTopic,
					Partition: kafka.PartitionAny,
				},
				Value: payload,
			}, nil)

			if err != nil {
				fmt.Printf("Failed to produce message: %v\n", err)
			} else {
				fmt.Printf("Enriched: %s (User: %s) (email: %s)\n", raw.AccountID, customerName, customerEmail)
			}
		} else {
			fmt.Printf("Consumer error: %v\n", err)
		}
	}
}
