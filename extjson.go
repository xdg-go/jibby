// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Efficient extended JSON detection:
//
// The longest extended JSON key is $regularExpression at 18 letters
// The shortest extended JSON key is $oid at 3 letters.  Any $-prefixed
// key outside those lengths isn't extended JSON.  We can switch on
// length to avoid a linear scan against all keys.
//
// $oid
// $code
// $date
// $type -- option for legacy $binary
// $scope
// $regex -- legacy regular expression
// $binary
// $maxKey
// $minKey
// $symbol
// $options -- option for legacy regular expression
// $dbPointer
// $numberInt
// $timestamp
// $undefined
// $numberLong
// $numberDouble
// $numberDecimal
// $regularExpression

// handleExtJSON is called from convertObject to potentially replace a JSON
// object with a non-document BSON value instead.  If it returns (nil, nil), it
// means that the input is not extended JSON and that no bytes were consumed
// from the input.  If it succeeds, then the extended JSON object will have been
// consumed.
//
// If/when a type is definitively determined, the `typeBytePos` of
// `out` will be overwritten the discovered type.
//
// This function generally only validates/dispatches to subroutines.  Those
// functions are responsible for consuming the full extended JSON object,
// including the closing terminator.
func (d *Decoder) handleExtJSON(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead for longest possible extjson key plus surrounding quotes.
	buf, err := d.json.Peek(20)
	if err != nil {
		// May have peeked to end of input, so EOF is OK.
		if err != io.EOF {
			return nil, err
		}
	}

	// Skip past opening quote (which must have existed to get to handleExtJSON)
	buf = buf[1:]

	// Common case: If $ doesn't follow opening quote, then not extended JSON.
	if len(buf) > 0 && buf[0] != '$' {
		return nil, nil
	}

	// Isolate key
	quotePos := bytes.IndexByte(buf, '"')
	if quotePos < 0 {
		// Key is longer than `$regularExpression"`, so not valid extended JSON.
		return nil, nil
	}
	key := buf[0:quotePos]

	// When we find a key, we can write a type byte and discard from the input
	// buffer the length of that key plus two for the surrounding quotes.  In
	// ambiguous cases, we can't assign a type or discard, so we defer that
	// to a corresponding subroutine.
	switch len(key) {
	case 4: // $oid
		if bytes.Equal(key, jsonOID) {
			overwriteTypeByte(out, typeBytePos, bsonObjectID)
			_, _ = d.json.Discard(6)
			return d.convertOID(out)
		}
		return nil, nil
	case 5: // $code $date $type
		if bytes.Equal(key, jsonCode) {
			// Still don't know if this is code or code w/scope, so can't
			// assign type yet, but we can consume the key.
			_, _ = d.json.Discard(7)
			return d.convertCode(out, typeBytePos)
		} else if bytes.Equal(key, jsonDate) {
			overwriteTypeByte(out, typeBytePos, bsonDateTime)
			_, _ = d.json.Discard(7)
			return d.convertDate(out)
		} else if bytes.Equal(key, jsonType) {
			// Still don't know if this is binary or a $type query operator, so
			// can't assign type or discard anything yet.
			return d.convertType(out, typeBytePos)
		}
		return nil, nil
	case 6: // $scope $regex
		if bytes.Equal(key, jsonScope) {
			overwriteTypeByte(out, typeBytePos, bsonCodeWithScope)
			_, _ = d.json.Discard(8)
			return d.convertScope(out)
		} else if bytes.Equal(key, jsonRegex) {
			// Still don't know if this is legacy $regex or a $regex query
			// operator so can't assign type or discard anything yet.
			return d.convertRegex(out, typeBytePos)
		}
		return nil, nil
	case 7: // $binary $maxKey $minKey $symbol
		if bytes.Equal(key, jsonBinary) {
			overwriteTypeByte(out, typeBytePos, bsonBinary)
			_, _ = d.json.Discard(9)
			return d.convertBinary(out)
		} else if bytes.Equal(key, jsonMaxKey) {
			overwriteTypeByte(out, typeBytePos, bsonMaxKey)
			_, _ = d.json.Discard(9)
			return d.convertMinMaxKey(out)
		} else if bytes.Equal(key, jsonMinKey) {
			overwriteTypeByte(out, typeBytePos, bsonMinKey)
			_, _ = d.json.Discard(9)
			return d.convertMinMaxKey(out)
		} else if bytes.Equal(key, jsonSymbol) {
			overwriteTypeByte(out, typeBytePos, bsonSymbol)
			_, _ = d.json.Discard(9)
			return d.convertSymbol(out)
		}
		return nil, nil
	case 8: // $options
		if bytes.Equal(key, jsonOptions) {
			// Still don't know if this is legacy $regex or non-extJSON
			// so can't assign type or discard anything yet.
			return d.convertOptions(out, typeBytePos)
		}
		return nil, nil
	case 10: // $dbPointer $numberInt $timestamp $undefined
		if bytes.Equal(key, jsonDbPointer) {
			overwriteTypeByte(out, typeBytePos, bsonDBPointer)
			_, _ = d.json.Discard(12)
			return d.convertDBPointer(out)
		}
		if bytes.Equal(key, jsonNumberInt) {
			overwriteTypeByte(out, typeBytePos, bsonInt32)
			_, _ = d.json.Discard(12)
			return d.convertNumberInt(out)
		}
		if bytes.Equal(key, jsonTimestamp) {
			overwriteTypeByte(out, typeBytePos, bsonTimestamp)
			_, _ = d.json.Discard(12)
			return d.convertTimestamp(out)
		}
		if bytes.Equal(key, jsonUndefined) {
			overwriteTypeByte(out, typeBytePos, bsonUndefined)
			_, _ = d.json.Discard(12)
			return d.convertUndefined(out)
		}
		return nil, nil
	case 11: // $numberLong
		if bytes.Equal(key, jsonNumberLong) {
			overwriteTypeByte(out, typeBytePos, bsonInt64)
			_, _ = d.json.Discard(13)
			return d.convertNumberLong(out)
		}
		return nil, nil
	case 13: // $numberDouble
		if bytes.Equal(key, jsonNumberDouble) {
			overwriteTypeByte(out, typeBytePos, bsonDouble)
			_, _ = d.json.Discard(15)
			return d.convertNumberDouble(out)
		}
		return nil, nil
	case 14: // $numberDecimal
		if bytes.Equal(key, jsonNumberDecimal) {
			overwriteTypeByte(out, typeBytePos, bsonDecimal128)
			_, _ = d.json.Discard(16)
			return d.convertNumberDecimal(out)
		}
		return nil, nil
	case 18: // $regularExpression
		if bytes.Equal(key, jsonRegularExpression) {
			overwriteTypeByte(out, typeBytePos, bsonRegex)
			_, _ = d.json.Discard(20)
			return d.convertRegularExpression(out)
		}
		return nil, nil
	default:
		// Not an extended JSON key
		return nil, nil
	}
}

// convertOID starts after the `"$oid"` key.
func (d *Decoder) convertOID(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// consume opening quote of string
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// peek ahead for 24 bytes and closing quote
	buf, err := d.json.Peek(25)
	if err != nil {
		return nil, newReadError(err)
	}
	if buf[24] != '"' {
		return nil, d.parseError(nil, "ill-formed $oid")
	}

	// extract hex string and convert/write
	var x [12]byte
	xs := x[0:12]
	_, err = hex.Decode(xs, buf[0:24])
	if err != nil {
		return nil, d.parseError(nil, fmt.Sprintf("objectID conversion: %v", err))
	}
	out = append(out, xs...)

	_, _ = d.json.Discard(25)

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertCode starts after the `"$code"` key.  We need to find out if it's just
// $code or followed by $scope to determine type byte.
//
// The problem with translation is that BSON code is just "string" and BSON code
// w/scope is "int32 string document", so we can't copy the code string to the
// BSON output until after we see if there is a $scope key so we know if we need
// to add the int32 length part.
func (d *Decoder) convertCode(out []byte, typeBytePos int) ([]byte, error) {
	// Whether code string or code w/scope, need to reserve at least 4 bytes for
	// a length, as both start that way.
	lengthPos := len(out)
	out = append(out, emptyLength...)

	// Consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	// Consume '"'
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Make a copy of code cstring to defer writing.
	codeCString := make([]byte, 0, 256)
	codeCString, err = d.convertCString(codeCString)
	if err != nil {
		return nil, err
	}

	// Look for value separator or object terminator.
	ch, err := d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}
	switch ch {
	case '}':
		// Just $code
		overwriteTypeByte(out, typeBytePos, bsonCode)
		out = append(out, codeCString...)
		// BSON code length is CString length, not including length bytes
		strLength := len(out) - lengthPos - 4
		overwriteLength(out, lengthPos, strLength)
	case ',':
		// Maybe followed by $scope
		err = d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		err = d.readSpecificKey(jsonScope)
		if err != nil {
			return nil, err
		}

		// We know it's code w/ scope: add additional length bytes for the
		// Cstring, then convert the scope object.
		overwriteTypeByte(out, typeBytePos, bsonCodeWithScope)
		strLengthPos := len(out)
		out = append(out, emptyLength...)
		out = append(out, codeCString...)
		strLength := len(out) - strLengthPos - 4
		overwriteLength(out, strLengthPos, strLength)

		err = d.readCharAfterWS('{')
		if err != nil {
			return nil, err
		}
		out, err = d.convertObject(out, topContainer)
		if err != nil {
			return nil, err
		}

		// BSON code w/scope length is total length including length bytes
		cwsLength := len(out) - lengthPos
		overwriteLength(out, lengthPos, cwsLength)

		// Must end with document terminator
		err = d.readObjectTerminator()
		if err != nil {
			return nil, err
		}
	default:
		return nil, d.parseError([]byte{ch}, "expected value separator or end of object")
	}

	return out, nil
}

// convertDate starts after the `"$date"` key.  The value might be
// an ISO-8601 string or might be a $numberLong object.
func (d *Decoder) convertDate(out []byte) ([]byte, error) {
	// Consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	ch, err := d.readAfterWS()
	if err != nil {
		return nil, err
	}
	switch ch {
	case '"':
		// Shortest ISO-8601 is `YYYY-MM-DDTHH:MM:SSZ` (20 chars); longest is
		// `YYYY-MM-DDTHH:MM:SS.sss+HH:MM` (29 chars).  Plus we need the closing
		// quote.  Peek a little further in case extra precision is given
		// (counter to the spec).
		buf, err := d.peekBoundedQuote(21, 48, "ISO 8601 datetime")
		if err != nil {
			return nil, err
		}
		epochMillis, err := parseISO8601toEpochMillis(buf)
		if err != nil {
			return nil, d.parseError(nil, err.Error())
		}
		_, _ = d.json.Discard(len(buf) + 1)
		var x [8]byte
		xs := x[0:8]
		binary.LittleEndian.PutUint64(xs, uint64(epochMillis))
		out = append(out, xs...)
	case '{':
		err = d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		err = d.readSpecificKey(jsonNumberLong)
		if err != nil {
			return nil, err
		}
		// readSpecificKey eats ':' but convertNumberLong wants it so unread it
		_ = d.json.UnreadByte()
		out, err = d.convertNumberLong(out)
		if err != nil {
			return nil, err
		}
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		_ = d.json.UnreadByte()
		// Unread and reread as Int64
		epochMillis, err := d.readInt64()
		if err != nil {
			return nil, err
		}
		var x [8]byte
		xs := x[0:8]
		binary.LittleEndian.PutUint64(xs, uint64(epochMillis))
		out = append(out, xs...)
	default:
		return nil, d.parseError([]byte{ch}, "invalid value for $date")
	}

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertType starts after the opening quote of the `"$type"` key.  We need to
// distinguish between Extended JSON $type or something else. We decode to a
// scratch buffer and look for exactly "$type" and "$binary".
func (d *Decoder) convertType(out []byte, typeBytePos int) ([]byte, error) {
	// Slow path: look for exactly $type and $binary
	var err error
	scratch := d.scratchPool.Get().([]byte)
	defer func() { d.scratchPool.Put(scratch) }()

	scratch = scratch[0:0]
	scratch, err = d.convertObject(scratch, topContainer)
	if err != nil {
		return nil, err
	}

	var sawBinary, sawType, sawOther int
	var binaryValue, subTypeValue bson.RawValue
	elements, err := bson.Raw(scratch).Elements()
	for _, e := range elements {
		switch e.Key() {
		case "$binary":
			sawBinary++
			binaryValue = e.Value()
		case "$type":
			sawType++
			subTypeValue = e.Value()
		default:
			sawOther++
		}
	}

	// If not exactly the extended JSON document we're looking for, copy scratch
	// to output as a BSON document.
	if sawBinary != 1 || sawType != 1 || sawOther != 0 ||
		binaryValue.Type != bsontype.String || subTypeValue.Type != bsontype.String {
		overwriteTypeByte(out, typeBytePos, bsonDocument)
		out = append(out, scratch...)
		return out, nil
	}

	// If we reach here, then confirmed this as a binary BSON type
	overwriteTypeByte(out, typeBytePos, bsonBinary)
	lengthPos := len(out)
	out = append(out, emptyLength...)

	// Append the subtype byte
	subType, err := decodeBinarySubType([]byte(subTypeValue.StringValue()))
	if err != nil {
		return nil, fmt.Errorf("error decoding binary $type: %s", err)
	}
	out = append(out, subType)

	// Decode the binary payload
	enc := base64.StdEncoding.WithPadding('=')
	payload, err := enc.DecodeString(binaryValue.StringValue())
	if err != nil {
		return nil, fmt.Errorf("error parsing base64 data: %s", err)
	}
	binLength := len(payload)

	// For binary subtype 2, the byte payload must have the length repeated,
	if subType == 2 {
		// Extend out with payload length bytes before writing payload
		innerLenPos := len(out)
		out = append(out, emptyLength...)
		overwriteLength(out, innerLenPos, binLength)
		binLength += 4
	}

	out = append(out, payload...)
	overwriteLength(out, lengthPos, binLength)

	// We don't check for object terminator here because it was already
	// consumed in reading the object into the scratch buffer.

	return out, nil
}

// convertBinarySubType starts after the opening quote of the string holding hex
// bytes of the value.
func (d *Decoder) convertBinarySubType(out []byte, subTypeBytePos int) ([]byte, byte, error) {
	subTypeBytes, err := d.peekBoundedQuote(2, 3, "binary subtype")
	if err != nil {
		return nil, 0, err
	}
	subType, err := decodeBinarySubType(subTypeBytes)
	if err != nil {
		return nil, 0, d.parseError(nil, err.Error())
	}
	overwriteTypeByte(out, subTypeBytePos, subType)
	_, _ = d.json.Discard(len(subTypeBytes) + 1)

	return out, subType, nil
}

// convertScope starts after the `"$scope"` key.  Having a leading $scope means
// this is code w/ scope, but we have to buffer the scope document and write it
// to the destination after we convert the $code string that follows.
func (d *Decoder) convertScope(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Reserve length bytes for full code w/ scope length
	cwsLengthPos := len(out)
	out = append(out, emptyLength...)

	// Copy $scope into a temporary BSON document
	scopeDoc := make([]byte, 0, 256)
	err = d.readCharAfterWS('{')
	if err != nil {
		return nil, err
	}
	scopeDoc, err = d.convertObject(scopeDoc, topContainer)
	if err != nil {
		return nil, err
	}

	// Find and copy $code to the output
	err = d.readCharAfterWS(',')
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	err = d.readSpecificKey(jsonCode)
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	out, err = d.convertString(out)
	if err != nil {
		return nil, err
	}

	// Write buffered $scope
	out = append(out, scopeDoc...)

	// BSON code w/scope length is total length including length bytes
	cwsLength := len(out) - cwsLengthPos
	overwriteLength(out, cwsLengthPos, cwsLength)

	return out, nil
}

var dollarRegexQueryOpRe = regexp.MustCompile(`^"\$regex"\s*:\s*\{`)

// convertRegex starts on the opening quote of `"$regex"`.  We need to
// distinguish between Extended JSON $regex or MongoDB $regex query operator.
// If we can peek far enough, we can check with regular expresssions.
//
// Both query and extended JSON allow { "$regex": "...", "$options": "..." } so
// we choose to treat that like extended JSON.  If converted to a BSON regular
// expression and sent as a query to a MongoDB and it will work either way.
// However, a Javascript query expression like { "$regex": /abc/ } will turn
// into extended JSON like { "$regex" : { <$regex or $regularExpression object>
// } }, so if we see that "$regex" is followed by an object, we treat that as a
// query.
func (d *Decoder) convertRegex(out []byte, typeBytePos int) ([]byte, error) {
	// Fast path: Peek ahead some amount and look for $regex with object, which
	// we know isn't extended JSON. 16 bytes is enough even with a little extra
	// white space.
	buf, err := d.json.Peek(16)
	if err != nil {
		// EOF is OK if we peeked to the end
		if err != io.EOF {
			return nil, err
		}
	}
	if dollarRegexQueryOpRe.Match(buf) {
		return nil, nil
	}

	return d.convertRegexOptionsSlowPath(out, typeBytePos)
}

// convertRegexOptionsSlowPath: Convert current object to a scratch BSON buffer.
// If it has exactly two keys, $regex and $options, and if both values are
// strings, then we can copy just those pieces into a BSON regex in the output.
// Otherwise, it's not extended JSON and we can copy the scratch buffer to the
// output as a document
func (d *Decoder) convertRegexOptionsSlowPath(out []byte, typeBytePos int) ([]byte, error) {

	var err error
	scratch := d.scratchPool.Get().([]byte)
	defer func() { d.scratchPool.Put(scratch) }()

	scratch = scratch[0:0]
	scratch, err = d.convertObject(scratch, topContainer)
	if err != nil {
		return nil, err
	}

	var sawRegex, sawOptions, sawOther int
	var regexValue, optionsValue bson.RawValue
	elements, err := bson.Raw(scratch).Elements()
	for _, e := range elements {
		switch e.Key() {
		case "$regex":
			sawRegex++
			regexValue = e.Value()
		case "$options":
			sawOptions++
			optionsValue = e.Value()
		default:
			sawOther++
		}
	}

	// If not exactly an extended JSON document, copy scratch to output as
	// a BSON document.
	if sawRegex != 1 || sawOptions != 1 || sawOther != 0 ||
		regexValue.Type != bsontype.String || optionsValue.Type != bsontype.String {
		overwriteTypeByte(out, typeBytePos, bsonDocument)
		out = append(out, scratch...)
		return out, nil
	}

	// If we reach here, then confirmed this as a regular expression BSON type.
	overwriteTypeByte(out, typeBytePos, bsonRegex)

	out = append(out, []byte(regexValue.StringValue())...)
	out = append(out, nullByte)

	opts := []byte(optionsValue.StringValue())
	if len(opts) > 1 {
		err = sortOptions(opts)
		if err != nil {
			return nil, err
		}
		out = append(out, opts...)
	}
	out = append(out, nullByte)

	// We don't check for object terminator here because it was already
	// consumed in reading the object into the scratch buffer.

	return out, nil
}

// convertBinary starts after the `"$binary"` key.  However, we have to
// determine if it is legacy extended JSON, where $binary holds the data and is
// followed by a $type field, or v2 extended JSON where $binary is followed by
// an object that holds the data and subtype.
func (d *Decoder) convertBinary(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Determine if this is v1 or v2 $binary
	ch, err := d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}

	switch ch {
	case '{':
		out, err = d.convertV2Binary(out)
		if err != nil {
			return nil, err
		}
	case '"':
		out, err = d.convertV1Binary(out)
		if err != nil {
			return nil, err
		}
	default:
		return nil, d.parseError([]byte{ch}, "expected object or string")
	}

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertV2Binary is called after the opening brace of the object.  V2 $binary
// must be an object with keys "base64" and "subType".
func (d *Decoder) convertV2Binary(out []byte) ([]byte, error) {
	// write a length placeholder and a subtype byte placeholder
	lengthPos := len(out)
	out = append(out, emptyLength...)
	subTypeBytePos := len(out)
	out = append(out, emptyType)

	// Need to see exactly 2 keys, subType and base64, in any order.
	var sawBase64 bool
	var sawSubType bool
	var subType byte
	for {
		// Read the opening quote of the key and peek the key.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		key, err := d.peekBoundedQuote(7, 8, "valid $binary document keys")
		if err != nil {
			return nil, err
		}

		switch {
		case bytes.Equal(key, jsonSubType):
			if sawSubType {
				return nil, d.parseError(nil, "subType repeated")
			}
			sawSubType = true
			_, _ = d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}
			out, subType, err = d.convertBinarySubType(out, subTypeBytePos)
			if err != nil {
				return nil, err
			}
			// If we haven't seen the other key, we expect to see a separator.
			if !sawBase64 {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Equal(key, jsonBase64):
			if sawBase64 {
				return nil, d.parseError(nil, "base64 repeated")
			}
			sawBase64 = true
			_, _ = d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}
			out, err = d.convertBase64(out)
			if err != nil {
				return nil, err
			}
			// If we haven't seen the other key, we expect to see a separator.
			if !sawSubType {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(nil, "invalid key for $binary document")
		}
		if sawBase64 && sawSubType {
			break
		}
	}

	// write length of binary payload (added length of the output minux 5 bytes
	// for length+type)
	binLength := len(out) - lengthPos - 5

	// For binary subtype 2, the byte payload must have the length repeated,
	// so we need to rearrange the output data.  We don't do this by default
	// so that the common case avoids copies.
	if subType == 2 {
		// Extend out by size of length bytes, then slide data down.
		out = append(out, emptyLength...)
		payloadPos := lengthPos + 5
		for i := len(out) - 5; i >= payloadPos; i-- {
			out[i+4] = out[i]
		}
		// Write length to start of payload and increment outer length
		overwriteLength(out, payloadPos, binLength)
		binLength += 4
	}

	overwriteLength(out, lengthPos, binLength)

	// Must end with document terminator
	err := d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertV2Binary is called after the opening quote of the base64 payload. v1
// $binary is a string, followed by "$type" and no other keys.
func (d *Decoder) convertV1Binary(out []byte) ([]byte, error) {
	// Write a length placeholder and a subtype byte placeholder
	lengthPos := len(out)
	out = append(out, emptyLength...)
	subTypeBytePos := len(out)
	out = append(out, emptyType)
	var subType byte

	// Read the payload
	out, err := d.convertBase64(out)
	if err != nil {
		return nil, err
	}

	// $type key must be next
	err = d.readCharAfterWS(',')
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	key, err := d.peekBoundedQuote(6, 6, "$type")
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, jsonType) {
		return nil, d.parseError(nil, "expected $type")
	}
	_, _ = d.json.Discard(len(key) + 1)
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	out, subType, err = d.convertBinarySubType(out, subTypeBytePos)

	if err != nil {
		return nil, err
	}

	// write length of binary payload (added length of the output minux 5 bytes
	// for length+type)
	binLength := len(out) - lengthPos - 5

	// For binary subtype 2, the byte payload must have the length repeated,
	// so we need to rearrange the output data.  We don't do this by default
	// so that the common case avoids copies.
	if subType == 2 {
		// Extend out by size of length bytes, then slide data down.
		out = append(out, emptyLength...)
		payloadPos := lengthPos + 5
		for i := len(out) - 5; i >= payloadPos; i-- {
			out[i+4] = out[i]
		}
		// Write length to start of payload and increment outer length
		overwriteLength(out, payloadPos, binLength)
		binLength += 4
	}

	overwriteLength(out, lengthPos, binLength)

	return out, nil
}

// convertMinMaxKey starts after the `"$minKey"` or `"$maxKey"` key.  In either
// case the type byte is already set and the only value for the key is `1` and
// no futher data has to be written. This function only validates and consumes
// input.
func (d *Decoder) convertMinMaxKey(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Rest must be `1` followed by object terminator
	err = d.readCharAfterWS('1')
	if err != nil {
		return nil, err
	}
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertSymbol starts after the `"$symbol"` key.
func (d *Decoder) convertSymbol(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Must have `"` followed by string followed by object terminator
	err = d.readCharAfterWS('"')
	if err != nil {
		return nil, err
	}
	out, err = d.convertString(out)
	if err != nil {
		return nil, err
	}
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

var dollarOptionsQueryOpRe = regexp.MustCompile(`^"\$options"\s*:\s*"[^"]*"\s*,\s*"\$regex"\s*:\s*\{`)

// convertOptions starts on the opening quote of `"$options"`.  We need to
// distinguish between Extended JSON $regex or MongoDB $regex query operator.
// If we can peek far enough, we can check with regular expresssions.
//
// See convertRegex for the logic differentiating the query and extended JSON
// forms.  Unlike that function, the regular expressions here must look past
// $option to find $regex to disambiguate.  If excessive white space makes the
// fast path regex fail to match, we'll still catch the correct case in the slow
// path.
func (d *Decoder) convertOptions(out []byte, typeBytePos int) ([]byte, error) {
	// Fast path: Peek ahead some amount and look for $regex with object, which
	// we know isn't extended JSON. 48 bytes is enough even with a little extra
	// white space to see `"$options" : "islmux", "$regex" : {`
	buf, err := d.json.Peek(48)
	if err != nil {
		// EOF is OK if we peeked to the end
		if err != io.EOF {
			return nil, err
		}
	}
	if dollarRegexQueryOpRe.Match(buf) {
		return nil, nil
	}

	return d.convertRegexOptionsSlowPath(out, typeBytePos)
}

// convertDBPointer starts after the `"$dbPointer"` key.  The value
// must be an object with two keys, $ref and $id.
func (d *Decoder) convertDBPointer(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Inner object
	err = d.readCharAfterWS('{')
	if err != nil {
		return nil, err
	}

	// Need to see exactly 2 keys, '$ref' and '$id', in any order.
	var ref []byte
	var id []byte
	var sawRef bool
	var sawID bool
	for {
		// Read the opening quote of the key and peek the key.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		key, err := d.peekBoundedQuote(4, 5, "valid $dbPointer document keys")
		if err != nil {
			return nil, err
		}

		// Handle the key.
		switch {
		case bytes.Equal(key, jsonRef):
			if sawRef {
				return nil, d.parseError(nil, "key '$ref' repeated")
			}
			sawRef = true
			_, _ = d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}

			ref = make([]byte, 0, 256)
			ref, err = d.convertString(ref)
			if err != nil {
				return nil, err
			}

			// If we haven't seen the other key, we expect to see a separator.
			if !sawID {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Equal(key, jsonID):
			if sawID {
				return nil, d.parseError(nil, "key '$id' repeated")
			}
			sawID = true
			_, _ = d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			// Value must be of type object ID.  Read the value into a temporary
			// buffer, reserving the first byte for discovered type.  Copy the
			// start of the reader for error reporting.
			peek := d.copyPeek(parseErrorContextLength)
			id = make([]byte, 1, 13)
			id, err = d.convertValue(id, 0)
			if err != nil {
				return nil, err
			}
			if id[0] != bsonObjectID {
				return nil, d.parseError(peek, fmt.Sprintf("$dbPointer.$id must be BSON type %d, not type %d", bsonObjectID, id[0]))
			}

			// If we haven't seen the other key, we expect to see a separator.
			if !sawRef {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(nil, "invalid key for $dbPointer document")
		}
		if sawRef && sawID {
			break
		}
	}

	// Write ref and id, in that order (skipping id type byte)
	out = append(out, ref...)
	out = append(out, id[1:]...)

	// Inner doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	// Outer doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertNumberInt starts after the `"$numberInt"` key.
func (d *Decoder) convertNumberInt(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Peek at least 2 and up to 12 chars (for '-2147483648' plus closing quote).
	buf, err := d.peekBoundedQuote(2, 12, "int32")
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(buf), 10, 32)
	if err != nil {
		return nil, d.parseError(nil, fmt.Sprintf("int conversion: %v", err))
	}
	var x [4]byte
	xs := x[0:4]
	binary.LittleEndian.PutUint32(xs, uint32(n))
	out = append(out, xs...)

	// Discard buffer and trailing quote
	_, _ = d.json.Discard(len(buf) + 1)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertTimestamp starts after the `"$timestamp"` key.  The value
// must be an object with two keys, t and i.
func (d *Decoder) convertTimestamp(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	// Require object start
	err = d.readCharAfterWS('{')
	if err != nil {
		return nil, err
	}

	// Need to see exactly 2 keys, 't' and 'i', in any order.
	var timestamp uint32
	var increment uint32
	var sawT bool
	var sawI bool
	for {
		// Read key and skip ahead to start of value.  Because both keys are the
		// same length, we can read instead of peeking.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		ch, err := d.json.ReadByte()
		if err != nil {
			return nil, newReadError(err)
		}

		// Handle the key.
		switch ch {
		case 't':
			if sawT {
				return nil, d.parseError([]byte{ch}, "key 't' repeated")
			}
			sawT = true
			err = d.readNextChar('"')
			if err != nil {
				return nil, err
			}
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.skipWS()
			if err != nil {
				return nil, err
			}
			timestamp, err = d.readUint32()
			if err != nil {
				return nil, err
			}
			// If we haven't seen the other key, we expect to see a separator.
			if !sawI {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case 'i':
			if sawI {
				return nil, d.parseError([]byte{ch}, "key 'i' repeated")
			}
			sawI = true
			err = d.readNextChar('"')
			if err != nil {
				return nil, err
			}
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.skipWS()
			if err != nil {
				return nil, err
			}

			increment, err = d.readUint32()
			if err != nil {
				return nil, err
			}
			// If we haven't seen the other key, we expect to see a separator.
			if !sawT {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError([]byte{ch}, "invalid key for $timestamp document")
		}
		if sawT && sawI {
			break
		}
	}

	// Write increment and timestamp in that order
	var x [4]byte
	xs := x[0:4]
	binary.LittleEndian.PutUint32(xs, increment)
	out = append(out, xs...)
	binary.LittleEndian.PutUint32(xs, timestamp)
	out = append(out, xs...)

	// Inner doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	// Outer doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertUndefined starts after the `"$undefined"` key.
func (d *Decoder) convertUndefined(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Rest must be `true` followed by object terminator
	err = d.readCharAfterWS('t')
	if err != nil {
		return nil, err
	}
	buf, err := d.json.Peek(3)
	if err != nil {
		return nil, newReadError(err)
	}
	if !bytes.Equal(buf, []byte{'r', 'u', 'e'}) {
		return nil, d.parseError([]byte{'t'}, "expected 'true'")
	}

	_, _ = d.json.Discard(3)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertNumberLong starts after the `"$numberLong"` key.
func (d *Decoder) convertNumberLong(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Peek at least 2 and up to 21 chars (for '-9223372036854775808' plus closing quote).
	buf, err := d.peekBoundedQuote(2, 21, "int64")
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return nil, d.parseError(nil, fmt.Sprintf("int conversion: %v", err))
	}
	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, uint64(n))
	out = append(out, xs...)

	// Discard buffer and trailing quote
	_, _ = d.json.Discard(len(buf) + 1)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertNumberDouble starts after the `"$numberDouble"` key.
func (d *Decoder) convertNumberDouble(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Peek at least 2 and up to doublePeekWidth chars (for long '0.0000...1' plus closing quote).
	buf, err := d.peekBoundedQuote(2, doublePeekWidth, "float64")
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return nil, d.parseError(nil, fmt.Sprintf("float conversion: %v", err))
	}

	var x [8]byte
	xs := x[0:8]
	// Go's NaN includes a payload, which is not canonical per the Extended JSON
	// specification, so we swap in the proper NaN.
	if math.IsNaN(n) {
		binary.LittleEndian.PutUint64(xs, canonicalNaN)
	} else {
		binary.LittleEndian.PutUint64(xs, math.Float64bits(n))
	}
	out = append(out, xs...)

	// Discard buffer and trailing quote
	_, _ = d.json.Discard(len(buf) + 1)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertNumberDecimal starts after the `"$numberDecimal"` key.
func (d *Decoder) convertNumberDecimal(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Peek at least 2 and up to decimalPeekWidth chars (for long '0.0000...1' plus closing quote).
	buf, err := d.peekBoundedQuote(2, decimalPeekWidth, "decimal128")
	if err != nil {
		return nil, err
	}

	d128, err := primitive.ParseDecimal128(string(buf))
	if err != nil {
		return nil, d.parseError(nil, "can't parse Decimal128")
	}

	hi, lo := d128.GetBytes()
	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, lo)
	out = append(out, xs...)
	binary.LittleEndian.PutUint64(xs, hi)
	out = append(out, xs...)

	// Discard buffer and trailing quote
	_, _ = d.json.Discard(len(buf) + 1)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertRegularExpression starts after the `"$regularExpression"` key.
// The value must be a document with two keys "pattern" and "options".
func (d *Decoder) convertRegularExpression(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Require object start
	err = d.readCharAfterWS('{')
	if err != nil {
		return nil, err
	}

	// Need to see exactly 2 keys, 'pattern' and 'options', in any order.
	var pattern []byte
	var options []byte
	var sawPattern bool
	var sawOptions bool
	for {
		// Read the opening quote of the key and peek the key.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		key, err := d.peekBoundedQuote(8, 8, "valid $regularExpression keys")
		if err != nil {
			return nil, err
		}

		// Handle the key.
		switch {
		case bytes.Equal(key, jsonREpattern):
			if sawPattern {
				return nil, d.parseError(nil, "key 'pattern' repeated")
			}
			sawPattern = true
			_, _ = d.json.Discard(len(key))
			err = d.readNextChar('"')
			if err != nil {
				return nil, err
			}
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}

			pattern = make([]byte, 0, 256)
			pattern, err = d.convertCString(pattern)
			if err != nil {
				return nil, err
			}

			// If we haven't seen the other key, we expect to see a separator.
			if !sawOptions {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Equal(key, jsonREoptions):
			if sawOptions {
				return nil, d.parseError(nil, "key 'options' repeated")
			}
			sawOptions = true
			_, _ = d.json.Discard(len(key))
			err = d.readNextChar('"')
			if err != nil {
				return nil, err
			}
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}

			// Copy options to separate data for validation/sorting.  Keep
			// a copy of the original reader for error reporting.
			peek := d.copyPeek(parseErrorContextLength)
			options = make([]byte, 0, 256)
			options, err = d.convertCString(options)
			if err != nil {
				return nil, err
			}

			// sort/validate
			if len(options) > 1 {
				err = sortOptions(options[0 : len(options)-1])
				if err != nil {
					return nil, d.parseError(peek, err.Error())
				}
			}

			// If we haven't seen the other key, we expect to see a separator.
			if !sawPattern {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(nil, "invalid key for $regularExpression document")
		}
		if sawPattern && sawOptions {
			break
		}
	}

	// Write pattern and options, in that order
	out = append(out, pattern...)
	out = append(out, options...)

	// Inner doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	// Outer doc must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// convertBase64 expects to start after an opening quote mark and consumes
// the string and closing quote.
func (d *Decoder) convertBase64(out []byte) ([]byte, error) {
	enc := base64.StdEncoding.WithPadding('=')
	var terminated bool
	var x [48]byte
	xs := x[0:48]

	for !terminated {
		// peek ahead 64 bytes.  N.B. Must be multiple of 4 because 4 base64
		// bytes become 3 decoded bytes.
		buf, err := d.json.Peek(64)
		if err != nil {
			// here, io.EOF is OK, since we're only peeking and may hit end of
			// object
			if err != io.EOF {
				return nil, err
			}
		}

		// if not enough chars, input ended before closing quote
		if len(buf) < 1 {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}

		// Look for closing quote.  If found, mark terminated and truncate buf
		// to match.
		quotePos := bytes.IndexByte(buf, '"')
		if quotePos >= 0 {
			terminated = true
			buf = buf[0:quotePos]
		}

		// If we have characters, decode and append them, then discard the
		// input.
		if len(buf) > 0 {
			n, err := enc.Decode(xs, buf)
			if err != nil {
				return nil, d.parseError(nil, fmt.Sprintf("error parsing base64 data: %s", err))
			}
			out = append(out, xs[0:n]...)
			_, _ = d.json.Discard(len(buf))
		}

		// If terminated, discard the closing quote.
		if terminated {
			_, _ = d.json.Discard(1)
		}
	}

	return out, nil
}

// Date conversion adapted from the MongoDB Go Driver: https://github.com/mongodb/mongo-go-driver
// Licensed under the Apache 2 license.
var timeFormats = []string{"2006-01-02T15:04:05.999Z07:00", "2006-01-02T15:04:05.999Z0700"}

func parseISO8601toEpochMillis(data []byte) (int64, error) {
	var t time.Time
	var err error
	for _, format := range timeFormats {
		t, err = time.Parse(format, string(data))
		if err == nil {
			break
		}
	}
	if err != nil {
		return 0, fmt.Errorf("invalid $date value string")
	}

	return t.Unix()*1e3 + int64(t.Nanosecond())/1e6, nil
}

func sortOptions(opts []byte) error {
	sort.Slice(opts, func(i int, j int) bool { return opts[i] < opts[j] })
	for i := range opts {
		switch opts[i] {
		case 'i', 'l', 'm', 's', 'u', 'x':
		default:
			return fmt.Errorf("invalid regular expression option '%c'", opts[i])
		}
	}
	return nil
}

// decodeBinarySubType converts a slice of bytes to a subType.  input must have
// at least one byte
func decodeBinarySubType(input []byte) (byte, error) {
	// Go requires even digits to decode hex.
	if len(input) == 1 {
		input = []byte{'0', input[0]}
	}
	var x [1]byte
	xs := x[0:1]
	_, err := hex.Decode(xs, input)
	if err != nil {
		return 0, fmt.Errorf("error parsing subtype: %v", err)
	}
	return xs[0], nil
}
