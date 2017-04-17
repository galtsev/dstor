package model

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Sample) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbzg uint32
	zbzg, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbzg > 0 {
		zbzg--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Tag":
			z.Tag, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Values":
			var zbai uint32
			zbai, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if zbai != 10 {
				err = msgp.ArrayError{Wanted: 10, Got: zbai}
				return
			}
			for zxvk := range z.Values {
				z.Values[zxvk], err = dc.ReadFloat64()
				if err != nil {
					return
				}
			}
		case "TS":
			z.TS, err = dc.ReadInt64()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Sample) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "Tag"
	err = en.Append(0x83, 0xa3, 0x54, 0x61, 0x67)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Tag)
	if err != nil {
		return
	}
	// write "Values"
	err = en.Append(0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(10)
	if err != nil {
		return
	}
	for zxvk := range z.Values {
		err = en.WriteFloat64(z.Values[zxvk])
		if err != nil {
			return
		}
	}
	// write "TS"
	err = en.Append(0xa2, 0x54, 0x53)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.TS)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Sample) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "Tag"
	o = append(o, 0x83, 0xa3, 0x54, 0x61, 0x67)
	o = msgp.AppendString(o, z.Tag)
	// string "Values"
	o = append(o, 0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, 10)
	for zxvk := range z.Values {
		o = msgp.AppendFloat64(o, z.Values[zxvk])
	}
	// string "TS"
	o = append(o, 0xa2, 0x54, 0x53)
	o = msgp.AppendInt64(o, z.TS)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Sample) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zcmr uint32
	zcmr, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zcmr > 0 {
		zcmr--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Tag":
			z.Tag, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Values":
			var zajw uint32
			zajw, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if zajw != 10 {
				err = msgp.ArrayError{Wanted: 10, Got: zajw}
				return
			}
			for zxvk := range z.Values {
				z.Values[zxvk], bts, err = msgp.ReadFloat64Bytes(bts)
				if err != nil {
					return
				}
			}
		case "TS":
			z.TS, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Sample) Msgsize() (s int) {
	s = 1 + 4 + msgp.StringPrefixSize + len(z.Tag) + 7 + msgp.ArrayHeaderSize + (10 * (msgp.Float64Size)) + 3 + msgp.Int64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Samples) DecodeMsg(dc *msgp.Reader) (err error) {
	var zcua uint32
	zcua, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zcua) {
		(*z) = (*z)[:zcua]
	} else {
		(*z) = make(Samples, zcua)
	}
	for zhct := range *z {
		err = (*z)[zhct].DecodeMsg(dc)
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Samples) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zxhx := range z {
		err = z[zxhx].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Samples) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zxhx := range z {
		o, err = z[zxhx].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Samples) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zdaf uint32
	zdaf, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zdaf) {
		(*z) = (*z)[:zdaf]
	} else {
		(*z) = make(Samples, zdaf)
	}
	for zlqf := range *z {
		bts, err = (*z)[zlqf].UnmarshalMsg(bts)
		if err != nil {
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Samples) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zpks := range z {
		s += z[zpks].Msgsize()
	}
	return
}
