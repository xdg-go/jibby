package jibby

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

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
	default:
		// Either a number or an error.  We can't write the type byte
		// until the number type is determined, so pass it down.
		err = d.json.UnreadByte()
		if err != nil {
			return nil, err
		}
		out, err = d.convertNumber(out, typeBytePos)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

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

	// Note position of placeholder for length that we write
	lengthPos := len(out)

	// Check for empty object or start of key
	ch, err = d.readAfterWS()
	if err != nil {
		return nil, newReadError(err)
	}
	switch ch {
	case '}':
		// Empty object
		out = append(out, nullByte)
		objectLength := len(out) - lengthPos
		overwriteLength(out, lengthPos, objectLength)
		return out, nil
	case '"':
		// If ExtJSON enabled and found, delegate value writing to it.  If it
		// doesn't return a byte buffer, the value wasn't extended JSON.
		if d.extJSONAllowed {
			buf, err := d.handleExtJSON(out, outerTypeBytePos)
			if err != nil {
				return nil, err
			}
			if buf != nil {
				return buf, nil
			}
		}

		// Not extended JSON, so now write the length placeholders and document
		// type byte.
		out = append(out, emptyLength...)
		overwriteTypeByte(out, outerTypeBytePos, bsonDocument)

		// Record position for the placeholder type byte that we write
		typeBytePos = len(out)
		out = append(out, emptyType)

		// Convert key as Cstring
		out, err = d.convertCString(out)
		if err != nil {
			return nil, err
		}
	default:
		return nil, d.parseError(ch, "expecting key or end of object")
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
				return nil, d.parseError(ch, "expecting key")
			}

			// If ExtJSON enabled and found, delegate value writing to it.  If it
			// doesn't return a byte buffer, the value wasn't extended JSON.
			if d.extJSONAllowed {
				buf, err := d.handleExtJSON(out, outerTypeBytePos)
				if err != nil {
					return nil, err
				}
				if buf != nil {
					return buf, nil
				}
			}

			// Not extended JSON, so now write the document type byte.
			overwriteTypeByte(out, outerTypeBytePos, bsonDocument)

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
			return nil, d.parseError(ch, "expecting value-separator or end of object")
		}
	}

	// Write null terminator and calculate/update length
	out = append(out, nullByte)
	objectLength := len(out) - lengthPos
	overwriteLength(out, lengthPos, objectLength)

	return out, nil
}

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
	err = d.json.UnreadByte()
	if err != nil {
		return nil, err
	}

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
			return nil, d.parseError(ch, "expecting value-separator or end of array")
		}
	}

	// Write null terminator and calculate/update length
	out = append(out, nullByte)
	arrayLength := len(out) - lengthPos
	overwriteLength(out, lengthPos, arrayLength)

	return out, nil
}

func (d *Decoder) convertTrue(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(3)
	if err != nil {
		return nil, newReadError(err)
	}
	// already saw 't', looking for "rue"
	if rest[0] != 'r' || rest[1] != 'u' || rest[2] != 'e' {
		return nil, d.parseError('t', "expecting true")
	}

	out = append(out, 1)

	_, err = d.json.Discard(3)
	if err != nil {
		return nil, fmt.Errorf("unexpected error discarding buffered reader: %v", err)
	}
	return out, nil
}

func (d *Decoder) convertFalse(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(4)
	if err != nil {
		return nil, newReadError(err)
	}
	// Already saw 'f', looking for "alse"
	if rest[0] != 'a' || rest[1] != 'l' || rest[2] != 's' || rest[3] != 'e' {
		return nil, d.parseError('f', "expecting false")
	}

	out = append(out, 0)

	_, err = d.json.Discard(4)
	if err != nil {
		return nil, fmt.Errorf("unexpected error discarding buffered reader: %v", err)
	}
	return out, nil
}

func (d *Decoder) convertNull(out []byte) ([]byte, error) {
	rest, err := d.json.Peek(3)
	if err != nil {
		return nil, newReadError(err)
	}
	// Already saw 'n', looking for "ull"
	if rest[0] != 'u' || rest[1] != 'l' || rest[2] != 'l' {
		return nil, d.parseError('n', "expecting null")
	}

	// Nothing to write

	_, err = d.json.Discard(3)
	if err != nil {
		return nil, fmt.Errorf("unexpected error discarding buffered reader: %v", err)
	}
	return out, nil
}

func (d *Decoder) convertNumber(out []byte, typeBytePos int) ([]byte, error) {
	var err error
	var isFloat bool
	var terminated bool

	buf, err := d.json.Peek(doublePeekWidth)
	if err != nil {
		// here, io.EOF is OK, since we're peeking and may hit end of
		// object
		if err != io.EOF {
			return nil, err
		}
	}

	// Find where the number appears to ends and if it's a float.
	var i int
LOOP:
	for i = 0; i < len(buf); i++ {
		switch buf[i] {
		case 'e', 'E', '.':
			isFloat = true
		case ' ', '\t', '\n', '\r', ',', ']', '}':
			terminated = true
			break LOOP
		}
	}

	if !terminated {
		if len(buf) < doublePeekWidth {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}
		return nil, d.parseError(buf[i-1], "number too long")
	}

	if isFloat {
		overwriteTypeByte(out, typeBytePos, bsonDouble)
		out, err = d.convertFloat(out, buf[0:i])
		if err != nil {
			return nil, err
		}
	} else {
		// Still don't know the type, so delegate.
		out, err = d.convertInt(out, typeBytePos, buf[0:i])
		if err != nil {
			return nil, err
		}
	}

	// i is at terminator or whitespace, so discard just before that.
	_, err = d.json.Discard(i)
	if err != nil {
		return nil, fmt.Errorf("unexpected error discarding buffered reader: %v", err)
	}
	return out, nil
}

func (d *Decoder) convertFloat(out []byte, buf []byte) ([]byte, error) {
	n, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return nil, fmt.Errorf("parser error: float conversion: %v", err)
	}

	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, math.Float64bits(n))
	out = append(out, xs...)
	return out, nil
}

func (d *Decoder) convertInt(out []byte, typeBytePos int, buf []byte) ([]byte, error) {
	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parser error: int conversion: %v", err)
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

func (d *Decoder) convertCString(out []byte) ([]byte, error) {
	var terminated bool
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
					// backslash is last char in peek buffer, so back up one
					// and break out to repeat peek from the backslash, this
					// time, needing at least two characters
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
					// "\uXXXX" needs 6 chars total from i, otherwise back up
					// and repeek, needing 6 chars
					if len(buf)-i < 6 {
						charsNeeded = 6
						break INNER
					}
					// convert next 4 bytes to rune and append it as UTF-8
					n, err := strconv.ParseInt(string(buf[i+2:i+6]), 16, 32)
					if err != nil {
						return nil, fmt.Errorf("parse error: converting unicode escape: %v", err)
					}
					out = append(out, []byte(string(n))...)
					i += 5
				default:
					return nil, fmt.Errorf("parse error: unknown escape '%s'", string(buf[i+1]))
				}
				// escape done, go back to needing only one char at a time
				charsNeeded = 1
			case '"':
				terminated = true
				break INNER
			default:
				out = append(out, buf[i])
			}
		}

		// If terminated, closing quote is at index i, so discard i + 1 bytes to include it,
		// otherwise only discard i bytes to skip the text we've copied.
		if terminated {
			_, err = d.json.Discard(i + 1)
		} else {
			_, err = d.json.Discard(i)
		}
		if err != nil {
			return nil, fmt.Errorf("unexpected error discarding buffered reader: %v", err)
		}
	}

	// C-string null terminator
	out = append(out, 0)

	return out, nil
}

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
