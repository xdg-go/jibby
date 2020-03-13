// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// convertValue starts before any bytes of a value have been read.  It detects
// the value's type and dispatches to a handler.  It supports all JSON value
// types. (The `convertObject` handler handles extended JSON.)
func (d *Decoder) convertValue(out []byte, typeBytePos int) ([]byte, error) {
	ch, err := d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}

	switch ch {
	case '{':
		// Defer writing type byte to handle extended JSON, which would have a
		// non-document type determined later from key inspection.
		out, err = d.convertObject(out, typeBytePos)
		if err != nil {
			return nil, err
		}
	case '[':
		overwriteTypeByte(out, typeBytePos, bsonArray)
		out, err = d.convertArray(out)
		if err != nil {
			return nil, err
		}
	case 't':
		overwriteTypeByte(out, typeBytePos, bsonBoolean)
		out, err = d.convertTrue(out)
		if err != nil {
			return nil, err
		}
	case 'f':
		overwriteTypeByte(out, typeBytePos, bsonBoolean)
		out, err = d.convertFalse(out)
		if err != nil {
			return nil, err
		}
	case 'n':
		overwriteTypeByte(out, typeBytePos, bsonNull)
		out, err = d.convertNull(out)
		if err != nil {
			return nil, err
		}
	case '"':
		overwriteTypeByte(out, typeBytePos, bsonString)
		out, err = d.convertString(out)
		if err != nil {
			return nil, err
		}
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// This starts a number, so we unread the byte and let that handler give
		// us any error.  We can't write the
		// type byte until the number type is determined (int64, int32, double),
		// so we pass in the type byte position.
		_ = d.json.UnreadByte()
		out, err = d.convertNumber(out, typeBytePos)
		if err != nil {
			return nil, err
		}
	default:
		return nil, d.parseError([]byte{ch}, "invalid character")
	}

	return out, nil
}

// convertObject starts after the opening brace of an object.
func (d *Decoder) convertObject(out []byte, outerTypeBytePos int) ([]byte, error) {
	var ch byte
	var err error
	var typeBytePos int

	// Depth check
	d.curDepth++
	if d.curDepth > d.maxDepth {
		return nil, errors.New("maximum depth exceeded")
	}
	defer func() { d.curDepth-- }()

	// Note position of placeholder for length that we write, but don't
	// write yet in case this turns out to be extended JSON.
	lengthPos := len(out)

	// Check for empty object or start of key
	ch, err = d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}
	switch ch {
	case '}':
		// Empty object
		overwriteTypeByte(out, outerTypeBytePos, bsonDocument)
		out = append(out, emptyDoc...)
		return out, nil
	case '"':
		// If ExtJSON enabled and `handleExtJSON` returns a buffer, then this
		// value was extended JSON and the value has been consumed.
		if d.extJSONAllowed && outerTypeBytePos != topContainer {
			// Put back quote so that handleExtJSON gets a valid start
			// for convertObject, as some types need to parse it to a scratch
			// buffer.
			_ = d.json.UnreadByte()
			buf, err := d.handleExtJSON(out, outerTypeBytePos)
			if err != nil {
				return nil, err
			}
			if buf != nil {
				return buf, nil
			}
			// Not extended JSON so re-read the quote we put back.
			_, _ = d.json.ReadByte()
		}

		// Not extended JSON, so now write the length placeholder
		out = append(out, emptyLength...)
		overwriteTypeByte(out, outerTypeBytePos, bsonDocument)

		// Record position for the placeholder type byte
		typeBytePos = len(out)
		out = append(out, emptyType)

		// Convert key as Cstring
		out, err = d.convertCString(out)
		if err != nil {
			return nil, err
		}
	default:
		return nil, d.parseError([]byte{ch}, "expecting key or end of object")
	}

	// Next non-WS char must be ':' for separator
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	// Convert first value of object
	out, err = d.convertValue(out, typeBytePos)
	if err != nil {
		return nil, err
	}

	// Loop, looking for separators or object terminator
LOOP:
	for {
		ch, err = d.readAfterWS()
		if err != nil {
			return nil, newReadError(err)
		}
		switch ch {
		case ',':
			// Next non-WS character must be quote to start key
			ch, err = d.readAfterWS()
			if err != nil {
				return nil, newReadError(err)
			}
			if ch != '"' {
				return nil, d.parseError([]byte{ch}, "expecting key")
			}

			// Record position for the placeholder type byte that we write
			typeBytePos = len(out)
			out = append(out, emptyType)

			// Convert key as Cstring
			out, err = d.convertCString(out)
			if err != nil {
				return nil, err
			}

			// Next non-WS char must be ':' for separator
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}

			// Convert next value
			out, err = d.convertValue(out, typeBytePos)
			if err != nil {
				return nil, err
			}
		case '}':
			break LOOP
		default:
			return nil, d.parseError([]byte{ch}, "expecting value-separator or end of object")
		}
	}

	// Write null terminator and calculate/update length
	out = append(out, nullByte)
	objectLength := len(out) - lengthPos
	overwriteLength(out, lengthPos, objectLength)

	return out, nil
}

// convertObject starts after the opening bracket of an array.
func (d *Decoder) convertArray(out []byte) ([]byte, error) {
	var ch byte
	var err error

	// Depth check
	d.curDepth++
	if d.curDepth > d.maxDepth {
		return nil, errors.New("maximum depth exceeded")
	}
	defer func() { d.curDepth-- }()

	// Note position of placeholder for length that we write
	lengthPos := len(out)
	out = append(out, emptyLength...)

	ch, err = d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}

	// Case: empty array
	if ch == ']' {
		out = append(out, nullByte)
		arrayLength := len(out) - lengthPos
		overwriteLength(out, lengthPos, arrayLength)
		return out, nil
	}

	// Not empty: unread the byte for convertValue to check
	_ = d.json.UnreadByte()

	// Record position for the placeholder type byte that we write
	typeBytePos := len(out)

	// Append type byte and empty string null terminator (no key)
	out = append(out, emptyType)

	// Start counting array entries for keys and append the first key
	index := 0
	out = append(out, arrayKey[index]...)
	out = append(out, nullByte)

	// Convert first value
	out, err = d.convertValue(out, typeBytePos)
	if err != nil {
		return nil, err
	}

	// Loop, looking for separators or array terminator
LOOP:
	for {
		ch, err = d.readAfterWS()
		if err != nil {
			return nil, newReadError(err)
		}

		switch ch {
		case ',':
			// Record position for the placeholder type byte that we write
			typeBytePos := len(out)

			// Append type byte
			out = append(out, emptyType)

			// Append next key
			index++
			if index < len(arrayKey) {
				out = append(out, arrayKey[index]...)
			} else {
				out = append(out, []byte(strconv.Itoa(index))...)
			}
			out = append(out, nullByte)

			// Convert next value
			out, err = d.convertValue(out, typeBytePos)
			if err != nil {
				return nil, err
			}
		case ']':
			break LOOP
		default:
			return nil, d.parseError([]byte{ch}, "expecting value-separator or end of array")
		}
	}

	// Write null terminator and calculate/update length
	out = append(out, nullByte)
	arrayLength := len(out) - lengthPos
	overwriteLength(out, lengthPos, arrayLength)

	return out, nil
}

// convertTrue starts after the 't' for true has been read.
func (d *Decoder) convertTrue(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(3)
	if err != nil {
		return nil, newReadError(err)
	}
	if len(rest) < 3 {
		return nil, newReadError(io.ErrUnexpectedEOF)
	}
	// already saw 't', looking for "rue"
	if rest[0] != 'r' || rest[1] != 'u' || rest[2] != 'e' {
		return nil, d.parseError([]byte{'t'}, "expecting true")
	}
	_, _ = d.json.Discard(3)

	out = append(out, 1)
	return out, nil
}

// convertTrue starts after the 'f' for false has been read.
func (d *Decoder) convertFalse(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(4)
	if err != nil {
		return nil, newReadError(err)
	}
	if len(rest) < 4 {
		return nil, newReadError(io.ErrUnexpectedEOF)
	}
	// Already saw 'f', looking for "alse"
	if rest[0] != 'a' || rest[1] != 'l' || rest[2] != 's' || rest[3] != 'e' {
		return nil, d.parseError([]byte{'f'}, "expecting false")
	}
	_, _ = d.json.Discard(4)

	out = append(out, 0)
	return out, nil
}

// convertTrue starts after the 'n' for null has been read.
func (d *Decoder) convertNull(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(3)
	if err != nil {
		return nil, newReadError(err)
	}
	if len(rest) < 3 {
		return nil, newReadError(io.ErrUnexpectedEOF)
	}
	// Already saw 'n', looking for "ull"
	if rest[0] != 'u' || rest[1] != 'l' || rest[2] != 'l' {
		return nil, d.parseError([]byte{'n'}, "expecting null")
	}
	_, _ = d.json.Discard(3)

	// Nothing to write

	return out, nil
}

// convertNumber starts before any of the number has been read.  Have to peek
// ahead in the buffer to find the end point and whether to convert as integer
// or floating point.  It consumes the number from input when finished.
func (d *Decoder) convertNumber(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead to find the bytes that make up the number representation.
	buf, isFloat, err := d.peekNumber()
	if err != nil {
		return nil, err
	}

	if isFloat {
		out, err = d.convertFloat(out, typeBytePos, buf)
		if err != nil {
			return nil, err
		}
	} else {
		// Still don't know if the type is int32 or int64, so delegate.
		out, err = d.convertInt(out, typeBytePos, buf)
		if err != nil {
			return nil, err
		}
	}

	_, _ = d.json.Discard(len(buf))

	return out, nil
}

// convertFloat converts the floating-point number in the buffer.  It does not
// consume any of the input.
func (d *Decoder) convertFloat(out []byte, typeBytePos int, buf []byte) ([]byte, error) {
	n, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return nil, d.parseError(nil, fmt.Sprintf("float conversion: %v", err))
	}

	overwriteTypeByte(out, typeBytePos, bsonDouble)
	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, math.Float64bits(n))
	out = append(out, xs...)
	return out, nil
}

// convertInt converts the integer number in the buffer.  It does not consume
// any of the input.
func (d *Decoder) convertInt(out []byte, typeBytePos int, buf []byte) ([]byte, error) {
	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		if strings.Contains(err.Error(), strconv.ErrRange.Error()) {
			// Doesn't fit in int64, so treat as float
			return d.convertFloat(out, typeBytePos, buf)
		}
		return nil, d.parseError(nil, fmt.Sprintf("int conversion: %v", err))
	}

	if n < math.MinInt32 || n > math.MaxInt32 {
		overwriteTypeByte(out, typeBytePos, bsonInt64)
		var x [8]byte
		xs := x[0:8]
		binary.LittleEndian.PutUint64(xs, uint64(n))
		out = append(out, xs...)
		return out, nil
	}

	var x [4]byte
	overwriteTypeByte(out, typeBytePos, bsonInt32)
	xs := x[0:4]
	binary.LittleEndian.PutUint32(xs, uint32(n))
	out = append(out, xs...)

	return out, nil
}

// convertCString starts after the opening quote of a string.  It peeks ahead in
// 64 byte chunks, consuming input when writing to the output.  It handles JSON
// string escape sequences, which may require consuming a partial chunk and
// peeking ahead to see the rest of an escape sequence.  When finished, the
// string and its closing quote have been consumed from the input.
func (d *Decoder) convertCString(out []byte) ([]byte, error) {
	var terminated bool

	// charsNeeded indicates how much we expect to peek ahead.  Normally, we
	// always expect to peek at least 1 ahead (for the closing quote), but when
	// handling escape sequences, that may change.  Being unable to peek the
	// desired amount ahead indicates unexpected EOF.
	var charsNeeded = 1

	for !terminated {
		// peek ahead 64 bytes
		buf, err := d.json.Peek(64)
		if err != nil {
			// here, io.EOF is OK, since we're only peeking and may hit end of
			// object
			if err != io.EOF {
				return nil, err
			}
		}

		// if not enough chars, input ended before closing quote or end of
		// escape sequence
		if len(buf) < charsNeeded {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}

		var i int
	INNER:
		for i = 0; i < len(buf); i++ {
			switch buf[i] {
			case '\\':
				// need at least two chars in buf
				if len(buf)-i < 2 {
					// not enough characters left, so break out to repeat peek
					// from the backslash but requiring at least two characters
					charsNeeded = 2
					break INNER
				}

				switch buf[i+1] {
				case '"', '\\', '/':
					out = append(out, buf[i+1])
					i++
				case 'b':
					out = append(out, '\b')
					i++
				case 'f':
					out = append(out, '\f')
					i++
				case 'n':
					out = append(out, '\n')
					i++
				case 'r':
					out = append(out, '\r')
					i++
				case 't':
					out = append(out, '\t')
					i++
				case 'u':
					// "\uXXXX" needs 6 chars total from i, otherwise break out
					// and repeek, needing 6 chars
					if len(buf)-i < 6 {
						charsNeeded = 6
						break INNER
					}
					// convert next 4 bytes to rune and append it as UTF-8
					n, err := strconv.ParseUint(string(buf[i+2:i+6]), 16, 32)
					if err != nil {
						_, _ = d.json.Discard(i)
						return nil, d.parseError(nil, fmt.Sprintf("converting unicode escape: %v", err))
					}
					out = append(out, []byte(string(n))...)
					i += 5
				default:
					msg := fmt.Sprintf("unknown escape '%s'", string(buf[i+1]))
					_, _ = d.json.Discard(i)
					return nil, d.parseError(nil, msg)
				}
				// Escape is done: go back to needing only one char at a time.
				charsNeeded = 1
			case '"':
				terminated = true
				break INNER
			default:
				if buf[i] < ' ' {
					_, _ = d.json.Discard(i)
					return nil, d.parseError(nil, "control characters not allowed in strings")
				}
				out = append(out, buf[i])
			}
		}

		// If terminated, closing quote is at index i, so discard i + 1 bytes to include it,
		// otherwise only discard i bytes to skip the text we've copied.
		if terminated {
			_, _ = d.json.Discard(i + 1)
		} else {
			_, _ = d.json.Discard(i)
		}
	}

	// C-string null terminator
	out = append(out, 0)

	return out, nil
}

// convertString starts after the opening quote of a string.  It works like
// convertCString except it prepends the length of the string.
func (d *Decoder) convertString(out []byte) ([]byte, error) {
	lengthPos := len(out)
	out = append(out, emptyLength...)

	out, err := d.convertCString(out)
	if err != nil {
		return nil, err
	}

	strLength := len(out) - lengthPos - 4
	overwriteLength(out, lengthPos, strLength)

	return out, nil
}
