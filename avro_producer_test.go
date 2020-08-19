package pulsavro

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/linkedin/goavro/v2"
	"log"
	"testing"
	"time"
)

// NewAvroProducer is a basic producer to interact with schema registry, avro and pulsar
func TestNewAvroProducer(t *testing.T) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Initialize pulsar client error: %v", err)
	}
	defer client.Close()
	producerOptions := pulsar.ProducerOptions{
		Topic: "public/default/test2",
	}
	schemaRegistryUrls := []string{"http://localhost:8080"}
	schema := `
        {
          "type": "record",
          "name": "TestDatum",
          "fields" : [
            {"name": "TestString", "type": ["null","string"],"default":null},
			{"name": "TestInt", "type": "int", "default": 0},
			{"name": "TestBool", "type": "boolean", "default": false}
          ]
        }`

	producer, err := NewAvroProducer(client, producerOptions, schemaRegistryUrls, schema)
	if err != nil {
		log.Fatalf("Initialize pulsar producer error: %v", err)
	}
	defer producer.Close()
	datum := map[string]interface{}{
		"TestString": goavro.Union("string", "Test"),
		"TestInt":    1,
		"TestBool":   true,
	}
	payload, err := producer.EncodeAvroMessage(datum)
	if err != nil {
		log.Fatalf("Encode avro message error: %v", err)
	}
	_, err = producer.Send(&pulsar.ProducerMessage{
		Payload: payload,
	})
	if err != nil {
		log.Fatalf("Send pulsar message error: %v", err)
	}
}
