package pulsar

import (
	"github.com/linkedin/goavro/v2"
)

func encodeAvroToBytes(schema string, datum interface{}) ([]byte, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	return codec.BinaryFromNative(nil, datum)
}

func decodeAvroFromBytes(schema string, payload []byte) (interface{}, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	datum, _, err := codec.NativeFromBinary(payload)
	return datum, err
}
