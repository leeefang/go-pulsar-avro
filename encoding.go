package pulsavro

import (
	"github.com/linkedin/goavro/v2"
)

// EncodeAvroToBytes convert interface{} datum to bytes
func EncodeAvroToBytes(schema string, datum interface{}) ([]byte, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	return codec.BinaryFromNative(nil, datum)
}

// DecodeAvroFromBytes convert bytes to interface{} datum
func DecodeAvroFromBytes(schema string, payload []byte) (interface{}, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	datum, _, err := codec.NativeFromBinary(payload)
	return datum, err
}
