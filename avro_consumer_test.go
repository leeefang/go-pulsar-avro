package pulsavro

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
	defer client.Close()
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
	defer consumer.Close()
	msg, err := consumer.Consumer.Receive(context.Background())
	if err != nil {
		log.Fatalf("Receive message error: %v", err)
	}
	log.Infof("message: Topic: %s, Payload: %x", msg.Topic(), msg.Payload())

	msgObj, err := consumer.DecodeAvroMessage(msg)
	if err != nil {
		log.Fatalf("Decode avro message error: %v", err)
	}
	log.Infof("msgObj: %v", msgObj)
	msgMap, ok := msgObj.(map[string]interface{})
	if ok {
		log.Fatal("message is not type of map[string]interface{}")
	}
	log.Info(msgMap["TestBool"])
}
