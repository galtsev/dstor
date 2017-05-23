package serializer

import (
	"fmt"
	. "github.com/galtsev/dstor/base"
	"github.com/mailru/easyjson"
	"github.com/tinylib/msgp/msgp"
)

type UnknownSerializer string

func (u UnknownSerializer) Error() string {
	return fmt.Sprintf("Unknown serializer: %s", u)
}

type Serializer interface {
	Marshal(v interface{}) []byte
	Unmarshal(data []byte, v interface{}) error
}

type EasyJsonSerializer struct{}

func (s EasyJsonSerializer) Marshal(v interface{}) []byte {
	data, err := easyjson.Marshal(v.(easyjson.Marshaler))
	Check(err)
	return data
}

func (s EasyJsonSerializer) Unmarshal(data []byte, v interface{}) error {
	return easyjson.Unmarshal(data, v.(easyjson.Unmarshaler))
}

func NewSerializer(name string) Serializer {
	switch name {
	case "easyjson":
		return EasyJsonSerializer{}
	case "msgp":
		return MsgPackSerializer{}
	default:
		panic(UnknownSerializer(name))
	}
}

type MsgPackSerializer struct{}

func (s MsgPackSerializer) Marshal(v interface{}) []byte {
	var buf []byte
	buf, err := v.(msgp.Marshaler).MarshalMsg(buf)
	Check(err)
	return buf
}

func (s MsgPackSerializer) Unmarshal(data []byte, v interface{}) error {
	_, err := v.(msgp.Unmarshaler).UnmarshalMsg(data)
	return err
}
