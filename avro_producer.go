package pulsavro

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
)

type AvroProducer struct {
	producer             pulsar.Producer
	schemaRegistryClient *CachedSchemaRegistryClient
	schema               string
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and pulsar
func NewAvroProducer(client pulsar.Client, producerOptions pulsar.ProducerOptions, schemaRegistryUrls []string, schema string) (*AvroProducer, error) {
	producer, err := client.CreateProducer(producerOptions)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryUrls)

	err = schemaRegistryClient.CreateSchemaByTopic(producerOptions.Topic, schema)
	if err != nil {
		producer.Close()
		return nil, err
	}

	return &AvroProducer{producer, schemaRegistryClient, schema}, nil
}

func (ap *AvroProducer) EncodeAvroMessage(value interface{}) ([]byte, error) {
	return EncodeAvroToBytes(ap.schema, value)
}

func (ap *AvroProducer) Send(message *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return ap.producer.Send(context.Background(), message)
}

func (ap *AvroProducer) Close() {
	ap.producer.Close()
}
