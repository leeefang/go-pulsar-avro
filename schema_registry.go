package pulsavro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/linkedin/goavro/v2"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

// SchemaRegistryClientInterface defines the api for all clients interfacing with schema registry
type SchemaRegistryClientInterface interface {
	GetSchemaCodecByTopic(string) (*goavro.Codec, error)
	CreateSchemaByTopic(string, string) error
}

// SchemaRegistryClient is a basic http client to interact with schema registry
type SchemaRegistryClient struct {
	SchemaRegistryConnect []string
	httpClient            *http.Client
	retries               int
}

type schemaResponse struct {
	Schema string `json:"data"`
}

const (
	schemaByTopic = "/admin/v2/schemas/%s/schema"
	contentType   = "application/json"
	timeout       = 2 * time.Second
)

// NewSchemaRegistryClient creates a client to talk with the schema registry at the connect string
// By default it will retry failed requests (5XX responses and http errors) len(connect) number of times
func NewSchemaRegistryClient(connect []string) *SchemaRegistryClient {
	client := &http.Client{
		Timeout: timeout,
	}
	return &SchemaRegistryClient{connect, client, len(connect)}
}

// NewSchemaRegistryClientWithRetries creates an http client with a configurable amount of retries on 5XX responses
func NewSchemaRegistryClientWithRetries(connect []string, retries int) *SchemaRegistryClient {
	client := &http.Client{
		Timeout: timeout,
	}
	return &SchemaRegistryClient{connect, client, retries}
}

// GetSchemaCodecByTopic returns a goavro.Codec by topic
func (client *SchemaRegistryClient) GetSchemaCodecByTopic(topic string) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(schemaByTopic, topic), nil)
	if nil != err {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if nil != err {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// CreateSchemaByTopic creates a schema for the specified topic
func (client *SchemaRegistryClient) CreateSchemaByTopic(topic string, schema string) error {
	buf := &bytes.Buffer{}
	err := json.Compact(buf, []byte(schema))
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"type": "JSON","schema": "%s","properties": {}}`, strings.ReplaceAll(strings.TrimSpace(buf.String()), `"`, `\"`))
	_, err = client.httpCall("POST", fmt.Sprintf(schemaByTopic, topic), strings.NewReader(payload))
	return err
}

func parseSchema(str []byte) (*schemaResponse, error) {
	var schema = new(schemaResponse)
	err := json.Unmarshal(str, &schema)
	return schema, err
}

func (client *SchemaRegistryClient) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	nServers := len(client.SchemaRegistryConnect)
	offset := rand.Intn(nServers)
	for i := 0; ; i++ {
		url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect[(i+offset)%nServers], uri)
		req, err := http.NewRequest(method, url, payload)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		resp, err := client.httpClient.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if i < client.retries && (err != nil || retryable(resp)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !isOK(resp) {
			return nil, newError(resp)
		}
		return ioutil.ReadAll(resp.Body)
	}
}

func retryable(resp *http.Response) bool {
	return resp.StatusCode >= 500 && resp.StatusCode < 600
}

func isOK(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
