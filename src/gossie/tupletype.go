package gossie

import (
	"fmt"
	"reflect"
	"strconv"
)

// tupleType is a GossieType that makes it easy to marshal any struct field
// to/from a Cassandra TupleType. It is enabled with `marshal:"tuple"`
//
// Example:
//
//  type Config struct {
//      Admin bool   `tuple:"0"`
//      TZ    string `tuple:"1"`
//  }
//  type User struct {
//      ID     string  `cf:"users" key:"ID"`
//      Config *Config `name:"config" marshal:"tuple"`
//  }

func tupleTypeBuilder(sf reflect.StructField) GossieType {
	rt := sf.Type
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	tt, err := newTupleType(rt)
	if err != nil {
		panic(err)
	}
	return tt
}

type tupleType struct {
	tupleOrderedFields []*field
}

type tupleMarshaler struct {
	v  interface{}
	tt *tupleType
}

func (t *tupleType) Marshaler(v interface{}, tagArgs *string) Marshaler {
	return &tupleMarshaler{v, t}
}

func (t *tupleType) Unmarshaler(v interface{}, tagArgs *string) Unmarshaler {
	return &tupleMarshaler{v, t}
}

func (m *tupleMarshaler) MarshalCassandra() ([]byte, error) {
	rv := reflect.ValueOf(m.v)
	for rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	tuple := make([]byte, 0)
	for _, f := range m.tt.tupleOrderedFields {
		if f == nil {
			return nil, fmt.Errorf("Cannot marshal if field in tuple skipped")
		}
		result, err := f.marshalValue(&rv)
		if err != nil {
			return nil, err
		}
		tuple = append(tuple, packTuple(result)...)
	}

	return tuple, nil
}

func (m *tupleMarshaler) UnmarshalCassandra(b []byte) error {
	rv := reflect.ValueOf(m.v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}
	components := unpackTuple(b)
	for i, f := range m.tt.tupleOrderedFields {
		// Skip any fields that are missing, they might have been added to the
		// schema after this row was created.
		if i >= len(components) {
			break
		}
		// Allow a tuple field to be skipped if only reading values
		if f == nil {
			continue
		}
		err := f.unmarshalValue(components[i], &rv)
		if err != nil {
			return err
		}
	}
	return nil
}

func newTupleType(t reflect.Type) (*tupleType, error) {
	tt := &tupleType{
		tupleOrderedFields: make([]*field, t.NumField()),
	}
	n := t.NumField()
	for i := 0; i < n; i++ {
		sf := t.Field(i)
		tupleTag := sf.Tag.Get("tuple")
		if tupleTag == "" {
			continue
		}
		tupleInt, err := strconv.Atoi(tupleTag)
		if err != nil {
			return nil, fmt.Errorf("Invalid tuple tag in struct: %v: %v", t.Name(), err)
		}
		if tupleInt >= len(tt.tupleOrderedFields) {
			// A tuple field must have been skipped, expand the slice to fit this index
			tt.tupleOrderedFields = append(tt.tupleOrderedFields, make([]*field, 1+tupleInt-len(tt.tupleOrderedFields))...)
		}
		if tt.tupleOrderedFields[tupleInt] != nil {
			return nil, fmt.Errorf("Tuple index repeated in struct %v: %v", t.Name(), tupleInt)
		}
		f, err := newField(i, sf)
		if err != nil {
			return nil, fmt.Errorf("Error in struct %v: %v", t.Name(), err)
		}
		if f != nil {
			tt.tupleOrderedFields[tupleInt] = f
		}
	}
	return tt, nil
}
