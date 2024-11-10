package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
)

func init() {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.server": "kafka-topics-kafka-1:9092",
	})
	if err != nil {
		log.Fatalf("cannot start kafka's admin client: %v\n", adminClient)
	}
	defer adminClient.Close()
	_, err = adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             "balance-updated",
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	})
	if err != nil {
		log.Fatalf("cannot create topic %v\n", err)
	}
}

func main() {

}
