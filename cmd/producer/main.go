package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Transaction struct {
	Value  float64 `json:"value"`
	FromId string  `json:"from_id"`
	ToId   string  `json:"to_id"`
}

func init() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers":   "kafka:9092",
		"delivery.timeout.ms": strconv.FormatInt((2 * time.Second).Milliseconds(), 10),
		"acks":                "all",
		"enable.idempotence":  "true",
	})
	if err != nil {
		slog.Error("cannot start kafka's admin client\n",
			slog.String("context", "init"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	defer adminClient.Close()
	_, err = adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             "balance-updated",
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	})
	if err != nil {
		slog.Error("cannot create topic \n",
			slog.String("context", "init"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	slog.Info("kafka connected and topic 'balance-updated' created")
}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
	})
	if err != nil {
		slog.Error("cannot create producer",
			slog.String("context", "create producer"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	defer producer.Close()
	topic := "balance-updated"
	maxF := 1000.00
	minF := 1.0
	randFloat := rand.Float64()*(maxF-minF) + minF
	valuePrecison2, err := strconv.ParseFloat(fmt.Sprintf("%.2f", randFloat), 64)
	if err != nil {
		slog.Error("cannot produce value",
			slog.String("context", "create transaction value"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	jsonBytes, err := json.Marshal(&Transaction{
		Value:  valuePrecison2,
		FromId: fmt.Sprintf("%d", rand.Int()),
		ToId:   fmt.Sprintf("%d", rand.Int()),
	})
	if err != nil {
		slog.Error("when trying to marshal Transaction",
			slog.String("context", "create message"),
			slog.String("error", err.Error()),
		)
		return
	}
	mgs := &kafka.Message{
		Value: jsonBytes,
		Key:   nil,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}
	deliveryChan := make(chan kafka.Event)
	err = producer.Produce(mgs, deliveryChan)
	if err != nil {
		slog.Error("cannot produce a message",
			slog.String("context", "create message"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	go deliveryReport(deliveryChan)
	producer.Flush(1000)
}

func deliveryReport(ch <-chan kafka.Event) {
	for event := range ch {
		message, ok := event.(*kafka.Message)
		if !ok {
			slog.Error("cannot publish message",
				slog.String("context", "delivery channel"),
				slog.String("error", message.TopicPartition.Error.Error()),
			)
			continue
		}
		slog.Info("message publish with success",
			slog.String("context", "delivery report"),
			slog.String("topic", *message.TopicPartition.Topic),
			slog.Int("partition", int(message.TopicPartition.Partition)),
			slog.String("offset", message.TopicPartition.Offset.String()),
		)
		// @todo save into database
	}
}
