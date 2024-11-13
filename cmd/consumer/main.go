package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log/slog"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		slog.Error("cannot create consumer",
			slog.String("context", "create consumer"),
			slog.String("error", err.Error()),
		)
		return
	}
	err = consumer.SubscribeTopics([]string{"balance-updated"}, nil)
	if err != nil {
		slog.Error("cannot subscribe to topics",
			slog.String("context", "subscribe to topics"),
			slog.String("error", err.Error()),
		)
		return
	}
	for {
		mgs, errMgs := consumer.ReadMessage(-1)
		if errMgs != nil {
			slog.Error("cannot read message",
				slog.String("context", "read message"),
				slog.String("error", err.Error()),
			)
			continue
		}
		slog.Info("reading message", string(mgs.Value))
	}
}
