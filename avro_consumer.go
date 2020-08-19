package pulsavro

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/linkedin/goavro/v2"
	"strings"
)

// AvroConsumer is a basic consumer to interact with schema registry, avro and pulsar
type AvroConsumer struct {
	Consumer             pulsar.Consumer
	schemaRegistryClient *CachedSchemaRegistryClient
}

func NewAvroConsumer(client pulsar.Client, consumerOptions pulsar.ConsumerOptions, schemaRegistryUrls []string) (*AvroConsumer, error) {
	consumer, err := client.Subscribe(consumerOptions)

	if err != nil {
		return nil, err
	}

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryUrls)
	return &AvroConsumer{
		consumer,
		schemaRegistryClient,
	}, nil
}

func (ac *AvroConsumer) GetSchemaByTopic(topic string) (*goavro.Codec, error) {
	return ac.schemaRegistryClient.GetSchemaCodecByTopic(topic)
}

func (ac *AvroConsumer) DecodeAvroMessage(msg pulsar.Message) (interface{}, error) {
	mTopic := strings.ReplaceAll(strings.ReplaceAll(msg.Topic(), "non-persistent://", ""), "persistent://", "")
	codec, err := ac.GetSchemaByTopic(mTopic)
	if err != nil {
		return nil, err
	}
	datum, _, err := codec.NativeFromBinary(msg.Payload())
	return datum, err
}

func (ac *AvroConsumer) Close() {
	ac.Consumer.Close()
}
