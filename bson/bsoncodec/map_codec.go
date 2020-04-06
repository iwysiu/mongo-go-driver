// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bsoncodec

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"go.mongodb.org/mongo-driver/bson/bsonoptions"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

var defaultMapCodec = NewMapCodec()

// MapCodec is the Codec used for map values.
type MapCodec struct {
	DecodeZerosMap   bool
	EncodeNilAsEmpty bool
	MgoKeyHandling   bool
}

var _ ValueCodec = &MapCodec{}

// NewMapCodec returns a MapCodec with options opts.
func NewMapCodec(opts ...*bsonoptions.MapCodecOptions) *MapCodec {
	mapOpt := bsonoptions.MergeMapCodecOptions(opts...)

	codec := MapCodec{}
	if mapOpt.DecodeZerosMap != nil {
		codec.DecodeZerosMap = *mapOpt.DecodeZerosMap
	}
	if mapOpt.EncodeNilAsEmpty != nil {
		codec.EncodeNilAsEmpty = *mapOpt.EncodeNilAsEmpty
	}
	if mapOpt.MgoKeyHandling != nil {
		codec.MgoKeyHandling = *mapOpt.MgoKeyHandling
	}
	return &codec
}

// EncodeValue is the ValueEncoder for map[*]* types.
func (mc *MapCodec) EncodeValue(ec EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Kind() != reflect.Map {
		return ValueEncoderError{Name: "MapEncodeValue", Kinds: []reflect.Kind{reflect.Map}, Received: val}
	}

	if val.IsNil() && !mc.EncodeNilAsEmpty {
		// If we have a nil map but we can't WriteNull, that means we're probably trying to encode
		// to a TopLevel document. We can't currently tell if this is what actually happened, but if
		// there's a deeper underlying problem, the error will also be returned from WriteDocument,
		// so just continue. The operations on a map reflection value are valid, so we can call
		// MapKeys within mapEncodeValue without a problem.
		err := vw.WriteNull()
		if err == nil {
			return nil
		}
	}

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	return mc.mapEncodeValue(ec, dw, val, nil)
}

// mapEncodeValue handles encoding of the values of a map. The collisionFn returns
// true if the provided key exists, this is mainly used for inline maps in the
// struct codec.
func (mc *MapCodec) mapEncodeValue(ec EncodeContext, dw bsonrw.DocumentWriter, val reflect.Value, collisionFn func(string) bool) error {

	elemType := val.Type().Elem()
	encoder, err := ec.LookupEncoder(elemType)
	if err != nil && elemType.Kind() != reflect.Interface {
		return err
	}

	keys := val.MapKeys()
	for _, key := range keys {
		keyStr, err := mc.encodeKey(key)
		if err != nil {
			return err
		}

		if collisionFn != nil && collisionFn(keyStr) {
			return fmt.Errorf("Key %s of inlined map conflicts with a struct field name", key)
		}

		currEncoder, currVal, lookupErr := defaultValueEncoders.lookupElementEncoder(ec, encoder, val.MapIndex(key))
		if lookupErr != nil && lookupErr != errInvalidValue {
			return lookupErr
		}

		vw, err := dw.WriteDocumentElement(keyStr)
		if err != nil {
			return err
		}

		if lookupErr == errInvalidValue {
			err = vw.WriteNull()
			if err != nil {
				return err
			}
			continue
		}

		if enc, ok := currEncoder.(ValueEncoder); ok {
			err = enc.EncodeValue(ec, vw, currVal)
			if err != nil {
				return err
			}
			continue
		}
		err = encoder.EncodeValue(ec, vw, currVal)
		if err != nil {
			return err
		}
	}

	return dw.WriteDocumentEnd()
}

// DecodeValue is the ValueDecoder for map[string/decimal]* types.
func (mc *MapCodec) DecodeValue(dc DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if val.Kind() != reflect.Map || (!val.CanSet() && val.IsNil()) {
		return ValueDecoderError{Name: "MapDecodeValue", Kinds: []reflect.Kind{reflect.Map}, Received: val}
	}

	switch vrType := vr.Type(); vrType {
	case bsontype.Type(0), bsontype.EmbeddedDocument:
	case bsontype.Null:
		val.Set(reflect.Zero(val.Type()))
		return vr.ReadNull()
	default:
		return fmt.Errorf("cannot decode %v into a %s", vrType, val.Type())
	}

	dr, err := vr.ReadDocument()
	if err != nil {
		return err
	}

	if val.IsNil() {
		val.Set(reflect.MakeMap(val.Type()))
	}

	if val.Len() > 0 && mc.DecodeZerosMap {
		clearMap(val)
	}

	eType := val.Type().Elem()
	decoder, err := dc.LookupDecoder(eType)
	if err != nil {
		return err
	}

	if eType == tEmpty {
		dc.Ancestor = val.Type()
	}

	keyType := val.Type().Key()

	for {
		key, vr, err := dr.ReadElement()
		if err == bsonrw.ErrEOD {
			break
		}
		if err != nil {
			return err
		}

		k, err := mc.decodeKey(key, keyType)
		if err != nil {
			return err
		}

		elem := reflect.New(eType).Elem()
		err = decoder.DecodeValue(dc, vr, elem)
		if err != nil {
			return err
		}

		val.SetMapIndex(k, elem)
	}
	return nil
}

func clearMap(m reflect.Value) {
	var none reflect.Value
	for _, k := range m.MapKeys() {
		m.SetMapIndex(k, none)
	}
}

func (mc *MapCodec) encodeKey(val reflect.Value) (string, error) {
	if mc.MgoKeyHandling {
		return fmt.Sprint(val), nil
	}

	// keys of any string type are used directly
	if val.Kind() == reflect.String {
		return val.String(), nil
	}
	// encoding.TextMarshalers are marshaled
	if tm, ok := val.Interface().(encoding.TextMarshaler); ok {
		if val.Kind() == reflect.Ptr && val.IsNil() {
			return "", nil
		}
		buf, err := tm.MarshalText()
		if err == nil {
			return string(buf), nil
		}
		return "", err
	}

	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	}
	return "", errors.New("unexpected map key type")
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

func (mc *MapCodec) decodeKey(key string, keyType reflect.Type) (reflect.Value, error) {
	keyVal := reflect.ValueOf(key)
	var err error
	switch {
	case !mc.MgoKeyHandling && reflect.PtrTo(keyType).Implements(textUnmarshalerType):
		keyVal = reflect.New(keyType)
		v := keyVal.Interface().(encoding.TextUnmarshaler)
		err = v.UnmarshalText([]byte(key))
		keyVal = keyVal.Elem()
	default:
		switch keyType.Kind() {
		case reflect.String:
			keyVal = reflect.ValueOf(key).Convert(keyType)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			s := string(key)
			n, parseErr := strconv.ParseInt(s, 10, 64)
			if parseErr != nil || reflect.Zero(keyType).OverflowInt(n) {
				err = fmt.Errorf("failed to unmarshal number key %v", s)
			}
			keyVal = reflect.ValueOf(n).Convert(keyType)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			s := string(key)
			n, parseErr := strconv.ParseUint(s, 10, 64)
			if parseErr != nil || reflect.Zero(keyType).OverflowUint(n) {
				err = fmt.Errorf("failed to unmarshal number key %v", s)
				break
			}
			keyVal = reflect.ValueOf(n).Convert(keyType)
		case reflect.Float32, reflect.Float64:
			if mc.MgoKeyHandling {
				parsed, err := strconv.ParseFloat(key, 64)
				if err != nil {
					return keyVal, fmt.Errorf("Map key is defined to be a decimal type (%v) but got error %v", keyType.Kind(), err)
				}
				keyVal = reflect.ValueOf(parsed)
				break
			}
			fallthrough
		default:
			return keyVal, errors.New("Unexpected key type")
		}
	}
	return keyVal, err
}
