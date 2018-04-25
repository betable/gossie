package gossie

import (
	"bytes"
	enc "encoding/binary"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/*
	to do:

	Int32Type

	IntegerType
	DecimalType

	don't assume int is int32 (tho it's prob ok)
	uints, support them?

	maybe add ascii/utf8 types support UUIDType, string native support?
	maybe some more (un)marshalings?

	more error checking, pass along all strconv errors
*/

const (
	_ = iota
	UnknownType
	BytesType
	AsciiType
	UTF8Type
	LongType
	Int32Type
	IntegerType
	DecimalType
	UUIDType
	TimeUUIDType
	LexicalUUIDType
	BooleanType
	FloatType
	DoubleType
	DateType
	CounterColumnType
	CompositeType
)

var (
	ErrorUnsupportedMarshaling                  = errors.New("Cannot marshal value")
	ErrorUnsupportedNilMarshaling               = errors.New("Cannot marshal nil")
	ErrorUnsupportedUnmarshaling                = errors.New("Cannot unmarshal value")
	ErrorUnsupportedNativeTypeUnmarshaling      = errors.New("Cannot unmarshal to native type")
	ErrorUnsupportedCassandraTypeUnmarshaling   = errors.New("Cannot unmarshal from Cassandra type")
	ErrorCassandraTypeSerializationUnmarshaling = errors.New("Cassandra serialization is wrong for the type, cannot unmarshal")
)

type TypeDesc int

type Marshaler interface {
	MarshalCassandra() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalCassandra([]byte) error
}

func Marshal(value interface{}, typeDesc TypeDesc) ([]byte, error) {
	// use custom marshaller
	if v, ok := value.(Marshaler); ok {
		return v.MarshalCassandra()
	}

	// plain nil case
	if value == nil {
		return nil, ErrorUnsupportedNilMarshaling
	}

	v := reflect.ValueOf(value)
	k := v.Kind()

	// Other kinds can also be nil (Chan, Func, Map, Interface, Slice)
	// But there's no support the direct marshal of those.
	if k == reflect.Ptr {
		if v.IsNil() {
			return nil, ErrorUnsupportedNilMarshaling
		}

		e := v.Elem()
		if e.CanInterface() == false {
			return nil, ErrorUnsupportedNilMarshaling
		}

		return Marshal(e.Interface(), typeDesc)
	}

	// Special marshalling for complex types
	switch i := v.Interface().(type) {
	case []byte:
		return i, nil
	case UUID:
		return marshalUUID(i, typeDesc)
	case time.Time:
		return marshalTime(i, typeDesc)
	}

	switch k {
	case reflect.Bool:
		return marshalBool(v.Bool(), typeDesc)
	case reflect.Int8:
		return marshalInt(v.Int(), 1, typeDesc)
	case reflect.Int16:
		return marshalInt(v.Int(), 2, typeDesc)
	case reflect.Int:
		return marshalInt(v.Int(), 4, typeDesc)
	case reflect.Int32:
		return marshalInt(v.Int(), 4, typeDesc)
	case reflect.Int64:
		return marshalInt(v.Int(), 8, typeDesc)
	case reflect.String:
		return marshalString(v.String(), typeDesc)
	case reflect.Float32:
		return marshalFloat32(float32(v.Float()), typeDesc)
	case reflect.Float64:
		return marshalFloat64(v.Float(), typeDesc)
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalBool(value bool, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {
	case BytesType, BooleanType:
		b := make([]byte, 1)
		if value {
			b[0] = 1
		}
		return b, nil

	case AsciiType, UTF8Type:
		b := make([]byte, 1)
		if value {
			b[0] = '1'
		} else {
			b[0] = '0'
		}
		return b, nil

	case LongType:
		b := make([]byte, 8)
		if value {
			b[7] = 1
		}
		return b, nil

	case Int32Type:
		b := make([]byte, 4)
		if value {
			b[3] = 1
		}
		return b, nil
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalInt(value int64, size int, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {

	case LongType:
		b := make([]byte, 8)
		enc.BigEndian.PutUint64(b, uint64(value))
		return b, nil

	case Int32Type:
		b := make([]byte, 4)
		enc.BigEndian.PutUint32(b, uint32(value))
		return b, nil

	case BytesType:
		b := make([]byte, 8)
		enc.BigEndian.PutUint64(b, uint64(value))
		return b[len(b)-size:], nil

	case DateType:
		if size != 8 {
			return nil, ErrorUnsupportedMarshaling
		}
		b := make([]byte, 8)
		enc.BigEndian.PutUint64(b, uint64(value))
		return b, nil

	case AsciiType, UTF8Type:
		return marshalString(strconv.FormatInt(value, 10), UTF8Type)
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalTime(value time.Time, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {
	// following Java conventions Cassandra standarizes this as millis
	case LongType, BytesType, DateType:
		valueI := value.UnixNano() / 1e6
		b := make([]byte, 8)
		enc.BigEndian.PutUint64(b, uint64(valueI))
		return b, nil

	// 32 bit, so assume regular unix time
	case Int32Type:
		valueI := value.Unix()
		b := make([]byte, 4)
		enc.BigEndian.PutUint32(b, uint32(valueI))
		return b, nil

	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalString(value string, typeDesc TypeDesc) ([]byte, error) {
	// let cassandra check the ascii-ness of the []byte
	switch typeDesc {
	case BytesType, AsciiType, UTF8Type:
		return []byte(value), nil

	case LongType:
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return marshalInt(i, 8, LongType)
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalUUID(value UUID, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {
	case BytesType, UUIDType, TimeUUIDType, LexicalUUIDType:
		return []byte(value[:]), nil
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalFloat32(value float32, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {
	case BytesType, FloatType:
		var b []byte
		buf := bytes.NewBuffer(b)
		enc.Write(buf, enc.BigEndian, value)
		return buf.Bytes(), nil
		/*
		   case DoubleType:
		       var valueD float64 = float64(value)
		       var b []byte
		       buf := bytes.NewBuffer(b)
		       enc.Write(buf, enc.BigEndian, valueD)
		       return buf.Bytes(), nil
		*/
	}
	return nil, ErrorUnsupportedMarshaling
}

func marshalFloat64(value float64, typeDesc TypeDesc) ([]byte, error) {
	switch typeDesc {
	case BytesType, DoubleType:
		var b []byte
		buf := bytes.NewBuffer(b)
		enc.Write(buf, enc.BigEndian, value)
		return buf.Bytes(), nil
		/*
		   case FloatType:
		       var valueF float32 = float32(value)
		       var b []byte
		       buf := bytes.NewBuffer(b)
		       enc.Write(buf, enc.BigEndian, valueF)
		       return buf.Bytes(), nil
		*/
	}
	return nil, ErrorUnsupportedMarshaling
}

func Unmarshal(b []byte, typeDesc TypeDesc, value interface{}) error {
	// use custom unmarshaller
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCassandra(b)
	}

	// Special marshalling for complex types
	switch i := value.(type) {
	case *[]byte:
		*i = b
		return nil
	case *UUID:
		return unmarshalUUID(b, typeDesc, i)
	case *time.Time:
		return unmarshalTime(b, typeDesc, i)
	}

	v := reflect.ValueOf(value)
	k := v.Kind()

	if k == reflect.Ptr {
		v = v.Elem()
		k = v.Kind()
	}

	if v.CanSet() == false {
		return ErrorUnsupportedNativeTypeUnmarshaling
	}

	switch k {
	case reflect.Bool:
		return unmarshalBool(b, typeDesc, v)
	case reflect.Int8:
		return unmarshalInt8(b, typeDesc, v)
	case reflect.Int16:
		return unmarshalInt16(b, typeDesc, v)
	case reflect.Int:
		return unmarshalInt32(b, typeDesc, v)
	case reflect.Int32:
		return unmarshalInt32(b, typeDesc, v)
	case reflect.Int64:
		return unmarshalInt64(b, typeDesc, v)
	case reflect.String:
		return unmarshalString(b, typeDesc, v)
	case reflect.Float32:
		return unmarshalFloat32(b, typeDesc, v)
	case reflect.Float64:
		return unmarshalFloat64(b, typeDesc, v)
	}
	return ErrorUnsupportedNativeTypeUnmarshaling
}

func unmarshalBool(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case BytesType, BooleanType:
		if len(b) < 1 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		if b[0] == 0 {
			value.SetBool(false)
		} else {
			value.SetBool(true)
		}
		return nil

	case AsciiType, UTF8Type:
		if len(b) < 1 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		if b[0] == '0' {
			value.SetBool(false)
		} else {
			value.SetBool(true)
		}
		return nil

	case LongType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		if b[7] == 0 {
			value.SetBool(false)
		} else {
			value.SetBool(true)
		}
		return nil

	case Int32Type:
		if len(b) != 4 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		if b[3] == 0 {
			value.SetBool(false)
		} else {
			value.SetBool(true)
		}
		return nil

	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalInt64(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case LongType, BytesType, DateType, CounterColumnType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(enc.BigEndian.Uint64(b)))
		return nil

	case AsciiType, UTF8Type:
		var r string
		rv := reflect.ValueOf(&r).Elem()
		err := unmarshalString(b, AsciiType, rv)
		if err != nil {
			return err
		}
		v, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return err
		}
		value.SetInt(v)
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalTime(b []byte, typeDesc TypeDesc, value *time.Time) error {
	switch typeDesc {
	case LongType, BytesType, DateType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		valueI := int64(enc.BigEndian.Uint64(b))
		// following Java conventions Cassandra standarizes this as millis
		*value = time.Unix(valueI/1000, (valueI%1000)*1e6)
		return nil

	case Int32Type:
		if len(b) != 4 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		valueI := int64(enc.BigEndian.Uint32(b))
		// 32 bit, so assume regular unix time
		*value = time.Unix(valueI*1e9, 0)
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalInt32(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case LongType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(int32(enc.BigEndian.Uint64(b))))
		return nil

	case BytesType, Int32Type:
		if len(b) != 4 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(int32(enc.BigEndian.Uint32(b))))
		return nil

	case AsciiType, UTF8Type:
		var r string
		rv := reflect.ValueOf(&r).Elem()
		err := unmarshalString(b, AsciiType, rv)
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(r)
		if err != nil {
			return err
		}
		value.SetInt(int64(i))
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalInt16(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case LongType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(int16(enc.BigEndian.Uint64(b))))
		return nil

	case Int32Type:
		if len(b) != 4 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(int16(enc.BigEndian.Uint32(b))))
		return nil

	case BytesType:
		if len(b) != 2 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(int16(enc.BigEndian.Uint16(b))))
		return nil

	case AsciiType, UTF8Type:
		var r string
		rv := reflect.ValueOf(&r).Elem()
		err := unmarshalString(b, AsciiType, rv)
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(r)
		if err != nil {
			return err
		}
		value.SetInt(int64(i))
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalInt8(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case LongType:
		if len(b) != 8 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(b[7]))
		return nil

	case Int32Type:
		if len(b) != 4 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(b[3]))
		return nil

	case BytesType:
		if len(b) != 1 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		value.SetInt(int64(b[0]))
		return nil

	case AsciiType, UTF8Type:
		var r string
		rv := reflect.ValueOf(&r).Elem()
		err := unmarshalString(b, AsciiType, rv)
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(r)
		if err != nil {
			return err
		}
		value.SetInt(int64(i))
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalString(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case BytesType, AsciiType, UTF8Type:
		value.SetString(string(b))
		return nil

	case LongType:
		var i int64
		iv := reflect.ValueOf(&i).Elem()
		err := unmarshalInt64(b, LongType, iv)
		if err != nil {
			return err
		}
		value.SetString(strconv.FormatInt(i, 10))
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalUUID(b []byte, typeDesc TypeDesc, value *UUID) error {
	switch typeDesc {
	case BytesType, UUIDType, TimeUUIDType, LexicalUUIDType:
		if len(b) != 16 {
			return ErrorCassandraTypeSerializationUnmarshaling
		}
		copy((*value)[:], b)
		return nil
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalFloat32(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case BytesType, FloatType:
		buf := bytes.NewBuffer(b)
		enc.Read(buf, enc.BigEndian, value.Addr().Interface())
		return nil
		/*
		   case DoubleType:
		       var valueD float64
		       buf := bytes.NewBuffer(b)
		       enc.Read(buf, enc.BigEndian, &valueD)
		       *value = float32(valueD)
		       return nil
		*/
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

func unmarshalFloat64(b []byte, typeDesc TypeDesc, value reflect.Value) error {
	switch typeDesc {
	case BytesType, DoubleType:
		buf := bytes.NewBuffer(b)
		enc.Read(buf, enc.BigEndian, value.Addr().Interface())
		return nil
		/*
		   case FloatType:
		       var valueF float32
		       buf := bytes.NewBuffer(b)
		       enc.Read(buf, enc.BigEndian, &valueF)
		       *value = float64(valueF)
		       return nil
		*/
	}
	return ErrorUnsupportedCassandraTypeUnmarshaling
}

type TypeClass struct {
	Desc       TypeDesc
	Components []TypeClass
	Reversed   bool
}

func extractReversed(cassType string) (string, bool) {
	reversed := false
	if strings.HasPrefix(cassType, "org.apache.cassandra.db.marshal.ReversedType(") {
		// extract the inner type
		cassType = cassType[strings.Index(cassType, "(")+1 : len(cassType)-1]
		reversed = true
	}
	return cassType, reversed
}

func parseTypeDesc(cassType string) TypeDesc {
	switch cassType {
	case "BytesType", "org.apache.cassandra.db.marshal.BytesType":
		return BytesType
	case "AsciiType", "org.apache.cassandra.db.marshal.AsciiType":
		return AsciiType
	case "UTF8Type", "org.apache.cassandra.db.marshal.UTF8Type":
		return UTF8Type
	case "LongType", "org.apache.cassandra.db.marshal.LongType":
		return LongType
	case "Int32Type", "org.apache.cassandra.db.marshal.Int32Type":
		return Int32Type
	case "IntegerType", "org.apache.cassandra.db.marshal.IntegerType":
		return IntegerType
	case "DecimalType", "org.apache.cassandra.db.marshal.DecimalType":
		return DecimalType
	case "UUIDType", "org.apache.cassandra.db.marshal.UUIDType":
		return UUIDType
	case "TimeUUIDType", "org.apache.cassandra.db.marshal.TimeUUIDType":
		return TimeUUIDType
	case "LexicalUUIDType", "org.apache.cassandra.db.marshal.LexicalUUIDType":
		return LexicalUUIDType
	case "BooleanType", "org.apache.cassandra.db.marshal.BooleanType":
		return BooleanType
	case "FloatType", "org.apache.cassandra.db.marshal.FloatType":
		return FloatType
	case "DoubleType", "org.apache.cassandra.db.marshal.DoubleType":
		return DoubleType
	case "DateType", "org.apache.cassandra.db.marshal.DateType":
		return DateType
	case "CounterColumnType", "org.apache.cassandra.db.marshal.CounterColumnType":
		return CounterColumnType
	}
	return BytesType
}

func parseTypeClass(cassType string) TypeClass {
	cassType, reversed := extractReversed(cassType)
	r := TypeClass{Reversed: reversed}

	// check for composite and parse it
	if strings.HasPrefix(cassType, "org.apache.cassandra.db.marshal.CompositeType(") {
		r.Desc = CompositeType
		componentsString := cassType[strings.Index(cassType, "(")+1 : len(cassType)-1]
		componentsSlice := strings.Split(componentsString, ",")
		var components []TypeClass
		for _, component := range componentsSlice {
			components = append(components, parseTypeClass(component))
		}
		r.Components = components
		return r
	}

	r.Desc = parseTypeDesc(cassType)

	return r
}

const (
	eocEquals  byte = 0
	eocGreater byte = 1
	eocLower   byte = 0xff
)

func packComposite(component []byte, eoc byte) []byte {
	r := make([]byte, 2)
	enc.BigEndian.PutUint16(r, uint16(len(component)))
	r = append(r, component...)
	return append(r, eoc)
}

func unpackComposite(composite []byte) [][]byte {
	components := make([][]byte, 0)
	for len(composite) > 0 {
		l := enc.BigEndian.Uint16(composite[:2])
		components = append(components, composite[2:2+l])
		composite = composite[3+l:]
	}
	return components
}

func packTuple(component []byte) []byte {
	r := make([]byte, 4)
	enc.BigEndian.PutUint32(r, uint32(len(component)))
	return append(r, component...)
}

func unpackTuple(tuple []byte) [][]byte {
	components := make([][]byte, 0)
	for len(tuple) > 0 {
		l := enc.BigEndian.Uint32(tuple[:4])
		components = append(components, tuple[4:4+l])
		tuple = tuple[4+l:]
	}
	return components
}

func defaultType(t reflect.Type) TypeDesc {
	switch t.Kind() {
	case reflect.Bool:
		return BooleanType
	case reflect.String:
		return UTF8Type
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return LongType
	case reflect.Float32:
		return FloatType
	case reflect.Float64:
		return DoubleType
	case reflect.Array:
		if t.Name() == "UUID" && t.Size() == 16 {
			return UUIDType
		}
		return UnknownType
	case reflect.Struct:
		if t.Name() == "Time" && t.PkgPath() == "time" {
			return DateType
		}
		return UnknownType
	case reflect.Slice:
		if et := t.Elem(); et.Kind() == reflect.Uint8 {
			return BytesType
		}
		return UnknownType
	}
	return UnknownType
}
