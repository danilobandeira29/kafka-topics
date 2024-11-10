package main

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
	"os"
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
		"bootstrap.servers": "kafka:9092",
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
			slog.String("context", "createproducer"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	defer producer.Close()
	topic := "balance-updated"
	jsonBytes, err := json.Marshal(&Transaction{
		Value:  40.0,
		FromId: "1",
		ToId:   "22",
	})
	if err != nil {
		slog.Error("when trying to marshal Transaction",
			slog.String("context", "createmessage"),
			slog.String("error", err.Error()),
		)
		return
	}
	err = producer.Produce(&kafka.Message{
		Value: jsonBytes,
		Key:   nil,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}, nil)
	if err != nil {
		slog.Error("cannot produce a message",
			slog.String("context", "createmessage"),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
	producer.Flush(1000)
}
