package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestNewAvroConsumer(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Initialize pulsar client error: %v", err)
	}
	topic := "public/default/test2"
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "test-consumer",
		Type:             pulsar.Shared,
	}
	schemaRegistryUrls := []string{"http://localhost:8080"}
	consumer, err := NewAvroConsumer(client, consumerOptions, schemaRegistryUrls)
	if err != nil {
		log.Fatalf("Initialize pulsar consumer error: %v", err)
	}

	msg, err := consumer.Consumer.Receive(context.Background())
	if err != nil {
		log.Fatalf("Receive message error: %v", err)
	}
	log.Infof("message: Topic: %s, Payload: %x", msg.Topic(), msg.Payload())

	msgObj, err := consumer.ParseMessage(msg)
	if err != nil {
		log.Fatalf("Parse message error: %v", err)
	}
	log.Infof("msgObj: %v", msgObj)

	log.Info(msgObj["TestBool"])
}
