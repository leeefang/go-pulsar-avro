package pulsavro

import (
	"github.com/linkedin/goavro/v2"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"reflect"
	"testing"
)

type NullableString map[string]interface{}

func NewNullableDatum(datum interface{}) NullableString {
	//n := make(map[string]interface{})
	//n["string"] = str
	//return n
	if datum == nil {
		return nil
	}
	return map[string]interface{}{"string": datum}
}
func strPtr(s string) *string {
	return &s
}
func TestEncodeAvroToBytes(t *testing.T) {

	type TestDatum struct {
		TestString interface{} //for union types
		TestInt    int
		TestBool   bool
	}
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

	testDatum1 := TestDatum{goavro.Union("string", "Test"), 1, true}
	datum := map[string]interface{}{}
	err := mapstructure.Decode(testDatum1, &datum)
	if err != nil {
		log.Fatalf("convert testDatum to map[string]interface{} error :%v", err)
	}

	log.Infof("datum: %v", datum)

	payload, err := EncodeAvroToBytes(schema, datum)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("payload: %x", payload)

	datum2, err := DecodeAvroFromBytes(schema, payload)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("datum: %v", datum2)

	testDatum2 := TestDatum{}
	err = mapstructure.Decode(datum2, &testDatum2)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("testDatum2: %v", testDatum2)
	s := reflect.ValueOf(testDatum2.TestString)
	log.Info(s)
}
