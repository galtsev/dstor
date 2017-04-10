// AUTOGENERATED FILE: easyjson marshaler/unmarshalers.

package model

import (
	json "encoding/json"
	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjsonC80ae7adDecodeDanPimcoV1Model(in *jlexer.Lexer, out *Sample) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "Tag":
			out.Tag = string(in.String())
		case "Values":
			if in.IsNull() {
				in.Skip()
				out.Values = nil
			} else {
				in.Delim('[')
				if out.Values == nil {
					if !in.IsDelim(']') {
						out.Values = make([]float64, 0, 8)
					} else {
						out.Values = []float64{}
					}
				} else {
					out.Values = (out.Values)[:0]
				}
				for !in.IsDelim(']') {
					var v1 float64
					v1 = float64(in.Float64())
					out.Values = append(out.Values, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		case "TS":
			out.TS = int64(in.Int64())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonC80ae7adEncodeDanPimcoV1Model(out *jwriter.Writer, in Sample) {
	out.RawByte('{')
	first := true
	_ = first
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Tag\":")
	out.String(string(in.Tag))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Values\":")
	if in.Values == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
		out.RawString("null")
	} else {
		out.RawByte('[')
		for v2, v3 := range in.Values {
			if v2 > 0 {
				out.RawByte(',')
			}
			out.Float64(float64(v3))
		}
		out.RawByte(']')
	}
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"TS\":")
	out.Int64(int64(in.TS))
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v Sample) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonC80ae7adEncodeDanPimcoV1Model(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Sample) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonC80ae7adEncodeDanPimcoV1Model(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *Sample) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonC80ae7adDecodeDanPimcoV1Model(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *Sample) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonC80ae7adDecodeDanPimcoV1Model(l, v)
}
