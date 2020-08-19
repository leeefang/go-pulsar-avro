package pulsavro

import (
	"github.com/linkedin/goavro/v2"
	"sync"
)

// CachedSchemaRegistryClient is a schema registry client that will cache some data to improve performance
type CachedSchemaRegistryClient struct {
	SchemaRegistryClient *SchemaRegistryClient
	schemaCache          map[string]*goavro.Codec
	schemaCacheLock      sync.RWMutex
	schemaIdCache        map[string]int
	schemaIdCacheLock    sync.RWMutex
}

func NewCachedSchemaRegistryClient(connect []string) *CachedSchemaRegistryClient {
	SchemaRegistryClient := NewSchemaRegistryClient(connect)
	return &CachedSchemaRegistryClient{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[string]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

func NewCachedSchemaRegistryClientWithRetries(connect []string, retries int) *CachedSchemaRegistryClient {
	SchemaRegistryClient := NewSchemaRegistryClientWithRetries(connect, retries)
	return &CachedSchemaRegistryClient{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[string]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

// GetSchemaCodecByTopic will return and cache the codec with the given topic information
func (client *CachedSchemaRegistryClient) GetSchemaCodecByTopic(topic string) (*goavro.Codec, error) {
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[topic]
	client.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	codec, err := client.SchemaRegistryClient.GetSchemaCodecByTopic(topic)
	if err != nil {
		return nil, err
	}
	client.schemaCacheLock.Lock()
	client.schemaCache[topic] = codec
	client.schemaCacheLock.Unlock()
	return codec, nil
}

// CreateSchemaByTopic will create a schema for the specified topic
func (client *CachedSchemaRegistryClient) CreateSchemaByTopic(topic string, schema string) error {
	return client.SchemaRegistryClient.CreateSchemaByTopic(topic, schema)
}
