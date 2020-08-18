package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/linkedin/goavro/v2"
	"strings"
)

type avroConsumer struct {
	Consumer             pulsar.Consumer
	SchemaRegistryClient *CachedSchemaRegistryClient
}

// avroConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewAvroConsumer(client pulsar.Client, consumerOptions pulsar.ConsumerOptions, schemaRegistryUrls []string) (*avroConsumer, error) {
	consumer, err := client.Subscribe(consumerOptions)

	if err != nil {
		return nil, err
	}

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryUrls)
	return &avroConsumer{
		consumer,
		schemaRegistryClient,
	}, nil
}

func (ac *avroConsumer) GetSchemaByTopic(topic string) (*goavro.Codec, error) {
	return ac.SchemaRegistryClient.GetSchemaCodecByTopic(topic)
}

func (ac *avroConsumer) ParseMessage(msg pulsar.Message) (map[string]interface{}, error) {
	mTopic := strings.ReplaceAll(strings.ReplaceAll(msg.Topic(), "non-persistent://", ""), "persistent://", "")
	codec, err := ac.GetSchemaByTopic(mTopic)
	if err != nil {
		return nil, err
	}
	datum, _, err := codec.NativeFromBinary(msg.Payload())
	if err != nil {
		return nil, err
	}
	return datum.(map[string]interface{}), nil
}

func (ac *avroConsumer) Close() {
	ac.Consumer.Close()
}
