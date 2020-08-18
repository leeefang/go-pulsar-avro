package pulsar

import (
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
	"testing"
)

type testDatum struct {
	testString string
	testInt    int
	testBool   bool
}

func TestEncodeAvroToBytes(t *testing.T) {
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
	//datum:=&testDatum{"TestString",1,true}
	datum := map[string]interface{}{
		"TestString": goavro.Union("string", "Test"),
		"TestInt":    1,
		"TestBool":   true,
	}
	payload, err := encodeAvroToBytes(schema, datum)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("payload: %x", payload)

	datum2, err := decodeAvroFromBytes(schema, payload)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("datum: %v", datum2)
}
