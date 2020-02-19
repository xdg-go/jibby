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
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Efficient extended JSON detection:
//
// The longest extended JSON key is $regularExpression at 18 letters
// The shortest extended JSON key is $oid at 3 letters.  Any $-prefixed
// key outside those lengths isn't extended JSON.

// $oid
// $code
// $date
// $type -- legacy $binary option
// $scope
// $regex -- legacy regular expression -- if document, not string, don't parse
// $binary
// $maxKey
// $minKey
// $symbol
// $options -- legacy regular expression; requires a string $options and $regex
// $dbPointer
// $numberInt
// $timestamp
// $undefined
// $numberLong
// $numberDouble
// $numberDecimal
// $regularExpression

func (d *Decoder) handleExtJSON(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead for longest possible extjson key plus closing quote.
	buf, err := d.json.Peek(19)
	if err != nil {
		// May have peeked to end of input, so EOF is OK.
		if err != io.EOF {
			return nil, err
		}
	}

	// Common case: not extended JSON.
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

	switch len(key) {
	case 4: // $oid
		if bytes.Compare(key, jsonOID) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonObjectID)
			d.json.Discard(5)
			return d.convertOID(out)
		}
		return nil, nil
	case 5: // $code $date $type
		if bytes.Compare(key, jsonCode) == 0 {
			// Still don't know if this is code or code w/scope, so can't
			// assign type yet.
			d.json.Discard(6)
			return d.convertCode(out, typeBytePos)
		} else if bytes.Compare(key, jsonDate) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonDateTime)
			d.json.Discard(6)
			return d.convertDate(out)
		} else if bytes.Compare(key, jsonType) == 0 {
			// Still don't know if this is binary or a $type query operator, so
			// can't assign type *or* discard anything yet.
			return d.convertType(out, typeBytePos)
		}
		return nil, nil
	case 6: // $scope $regex
		if bytes.Compare(key, jsonScope) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonCodeWithScope)
			d.json.Discard(7)
			return d.convertScope(out)
		} else if bytes.Compare(key, jsonRegex) == 0 {
			// Still don't know if this is legacy $regex or a $regex query
			// operator so can't assign type or discard yet.
			return d.convertRegex(out, typeBytePos)
		}
		return nil, nil
	case 7: // $binary $maxKey $minKey $symbol
		if bytes.Compare(key, jsonBinary) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonBinary)
			d.json.Discard(8)
			return d.convertBinary(out)
		} else if bytes.Compare(key, jsonMaxKey) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonMaxKey)
			d.json.Discard(8)
			return d.convertMinMaxKey(out)
		} else if bytes.Compare(key, jsonMinKey) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonMinKey)
			d.json.Discard(8)
			return d.convertMinMaxKey(out)
		} else if bytes.Compare(key, jsonSymbol) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonSymbol)
			d.json.Discard(8)
			return d.convertSymbol(out)
		}
		return nil, nil
	case 8: // $options
		if bytes.Compare(key, jsonOptions) == 0 {
			// Still don't know if this is legacy $regex or non-extJSON
			// so can't assign type or discard yet.
			return d.convertOptions(out, typeBytePos)
		}
		return nil, nil
	case 10: // $dbPointer $numberInt $timestamp $undefined
		if bytes.Compare(key, jsonDbPointer) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonDBPointer)
			d.json.Discard(11)
			return d.convertDBPointer(out)
		}
		if bytes.Compare(key, jsonNumberInt) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonInt32)
			d.json.Discard(11)
			return d.convertNumberInt(out)
		}
		if bytes.Compare(key, jsonTimestamp) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonTimestamp)
			d.json.Discard(11)
			return d.convertTimestamp(out)
		}
		if bytes.Compare(key, jsonUndefined) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonUndefined)
			d.json.Discard(11)
			return d.convertUndefined(out)
		}
		return nil, nil
	case 11: // $numberLong
		if bytes.Compare(key, jsonNumberLong) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonInt64)
			d.json.Discard(12)
			return d.convertNumberLong(out)
		}
		return nil, nil
	case 13: // $numberDouble
		if bytes.Compare(key, jsonNumberDouble) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonDouble)
			d.json.Discard(14)
			return d.convertNumberDouble(out)
		}
		return nil, nil
	case 14: // $numberDecimal
		if bytes.Compare(key, jsonNumberDecimal) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonDecimal128)
			d.json.Discard(15)
			return d.convertNumberDecimal(out)
		}
		return nil, nil
	case 18: // $regularExpression
		if bytes.Compare(key, jsonRegularExpression) == 0 {
			overwriteTypeByte(out, typeBytePos, bsonRegex)
			d.json.Discard(19)
			return d.convertRegularExpression(out)
		}
		return nil, nil
	default:
		// Not an extended JSON key
		return nil, nil
	}
}

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
		return nil, d.parseError(buf[24], "ill-formed $oid")
	}

	// extract hex string and convert/write
	var x [12]byte
	xs := x[0:12]
	_, err = hex.Decode(xs, buf[0:24])
	if err != nil {
		return nil, fmt.Errorf("parser error: objectID conversion: %v", err)
	}
	out = append(out, xs...)

	// look for object close
	d.json.Discard(25)
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Starts after `"$code"`.  Need to find out if it's just $code or followed by
// $scope to determine type byte.
//
// Problem is that code is just "string" and code w/scope is "int32 string
// document", so we can't copy code string to output until after we see if there
// is a $scope key so we know if we need the int32 length part.
func (d *Decoder) convertCode(out []byte, typeBytePos int) ([]byte, error) {
	// Whether code string or code w/scope, need to reserve 4 bytes for a length.
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

	// Make a copy of code cstring to defer writing
	codeCString := make([]byte, 0, 256)
	codeCString, err = d.convertCString(codeCString)
	if err != nil {
		return nil, err
	}

	// Look for value separator or object terminator
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

		// We know it's code w/ scope: write Cstring with length, then object
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
	default:
		return nil, d.parseError(ch, "expected value separator or end of object")
	}

	return out, nil
}

func (d *Decoder) convertDate(out []byte) ([]byte, error) {
	// consume ':'
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
		// shortest ISO-8601 is `YYYY-MM-DDTHH:MM:SSZ` (20 chars); longest is
		// `YYYY-MM-DDTHH:MM:SS.sss+HH:MM` (29 chars).  Plus we need the closing
		// quote.  Peek a little further in case extra precision is given
		// (counter to the spec)
		buf, err := d.peekBoundedQuote(21, 48)
		if err != nil {
			return nil, err
		}
		epochMillis, err := parseISO8601toEpochMillis(buf)
		if err != nil {
			return nil, fmt.Errorf("parse error: %v", err)
		}
		d.json.Discard(len(buf) + 1)
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
		d.json.UnreadByte()
		out, err = d.convertNumberLong(out)
		if err != nil {
			return nil, err
		}
	}

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Starts after `"` of `"$type"` for key.  Need to distinguish between
// Extended JSON $type or MongoDB $type query operator.
// If we can peek far enough, we can check with regular expresssions.

var dollarTypeExtJSONRe = regexp.MustCompile(`\$type"\s*:\s*"\d\d?"`)
var dollarTypeQueryOpRe = regexp.MustCompile(`\$type"\s*:\s*(\d+|"\w+"|\{)`)

func (d *Decoder) convertType(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead successively longer; shouldn't be necessary but
	// covers a pathological case with excessive white space
	var isExtJSON bool
	peekDistance := 64
	var err error
	var buf []byte
	for peekDistance < d.json.Size() {
		buf, err = d.json.Peek(peekDistance)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
		}
		// Smallest possible valid buffer is 10 chars: `$type":"0"`
		if len(buf) < 10 {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}
		if dollarTypeExtJSONRe.Match(buf) {
			isExtJSON = true
			break
		} else if dollarTypeQueryOpRe.Match(buf) {
			// Signal not extended JSON with double nil.
			return nil, nil
		}
		if len(buf) < peekDistance {
			break
		}
		peekDistance *= 2
	}
	if !isExtJSON {
		return nil, fmt.Errorf("parse error: could not find value for $type within buffer lookahead starting at %q", string(buf))
	}

	// Write the type byte and hold space for length and subtype
	overwriteTypeByte(out, typeBytePos, bsonBinary)
	lengthPos := len(out)
	out = append(out, emptyLength...)
	subTypeBytePos := len(out)
	out = append(out, emptyType)

	// Discard $type key and closing quote
	d.json.Discard(6)
	// Read name separator and opening quote
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	// Read type bytes, decode, and write them
	out, err = d.convertBinarySubType(out, subTypeBytePos)
	if err != nil {
		return nil, err
	}

	// $binary key must be next
	err = d.readCharAfterWS(',')
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	err = d.readSpecificKey(jsonBinary)
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

	// write length of binary payload (added length minux 5 bytes for
	// length+type)
	binLength := len(out) - lengthPos - 5
	overwriteLength(out, lengthPos, binLength)

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (d *Decoder) convertBinarySubType(out []byte, subTypeBytePos int) ([]byte, error) {
	subTypeBytes, err := d.peekBoundedQuote(2, 3)
	if err != nil {
		return nil, err
	}
	// Go requires even digits to decode hex.
	normalizedSubType := subTypeBytes
	if len(normalizedSubType) == 1 {
		normalizedSubType = []byte{'0', normalizedSubType[0]}
	}
	var x [1]byte
	xs := x[0:1]
	_, err = hex.Decode(xs, normalizedSubType)
	if err != nil {
		return nil, d.parseError(subTypeBytes[0], fmt.Sprintf("error parsing subtype: %v", err))
	}
	overwriteTypeByte(out, subTypeBytePos, xs[0])
	d.json.Discard(len(subTypeBytes) + 1)

	return out, nil
}

// Leading $scope means code w/ scope, but we have to buffer the scope
// document and write it after we convert the code string
func (d *Decoder) convertScope(out []byte) ([]byte, error) {
	// consume ':'
	err := d.readNameSeparator()
	if err != nil {
		return nil, err
	}

	cwsLengthPos := len(out)
	out = append(out, emptyLength...)

	scopeDoc := make([]byte, 0, 256)
	err = d.readCharAfterWS('{')
	if err != nil {
		return nil, err
	}
	scopeDoc, err = d.convertObject(scopeDoc, topContainer)
	if err != nil {
		return nil, err
	}

	// Read $code
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

	// Write buffered scope
	out = append(out, scopeDoc...)

	// BSON code w/scope length is total length including length bytes
	cwsLength := len(out) - cwsLengthPos
	overwriteLength(out, cwsLengthPos, cwsLength)

	return out, nil
}

var dollarRegexExtJSONRe = regexp.MustCompile(`^\$regex"\s*:\s*"`)
var dollarRegexQueryOpRe = regexp.MustCompile(`^\$regex"\s*:\s*\{`)

func (d *Decoder) convertRegex(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead successively longer; shouldn't be necessary but
	// covers a pathological case with excessive white space
	peekDistance := 64
	var isExtJSON bool
	var err error
	var buf []byte
	for peekDistance < d.json.Size() {
		buf, err = d.json.Peek(peekDistance)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
		}
		// Smallest possible matching buffer is 9 chars: `$regex":{`
		if len(buf) < 9 {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}
		if dollarRegexExtJSONRe.Match(buf) {
			isExtJSON = true
			break
		} else if dollarRegexQueryOpRe.Match(buf) {
			// Signal not extended JSON with double nil.
			return nil, nil
		}
		if len(buf) < peekDistance {
			break
		}
		peekDistance *= 2
	}
	if !isExtJSON {
		return nil, fmt.Errorf("parse error: invalid $regex Extended JSON at %q", string(buf))
	}

	overwriteTypeByte(out, typeBytePos, bsonRegex)

	// Discard $regex key and closing quote
	d.json.Discard(7)
	// Read name separator and opening quote
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}

	// Read regex pattern
	out, err = d.convertCString(out)
	if err != nil {
		return nil, err
	}

	// $options key must be next
	err = d.readCharAfterWS(',')
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	err = d.readSpecificKey(jsonOptions)
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	// Read options string
	out, err = d.convertCString(out)
	if err != nil {
		return nil, err
	}

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
		return nil, d.parseError(ch, "expected object or string")
	}

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// v2 $binary is a document with keys "base64" and "subType".
// This function is called after the opening bracket is already read.
func (d *Decoder) convertV2Binary(out []byte) ([]byte, error) {
	// write a length placeholder and a type placeholder
	lengthPos := len(out)
	out = append(out, emptyLength...)
	subTypeBytePos := len(out)
	out = append(out, emptyType)

	// Need to see exactly 2 keys, subType and base64, in any order.
	var sawBase64 bool
	var sawSubType bool
	for {
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}

		key, err := d.peekBoundedQuote(7, 8)
		if err != nil {
			return nil, err
		}
		switch {
		case bytes.Compare(key, jsonSubType) == 0:
			if sawSubType {
				return nil, d.parseError(key[0], "subType repeated")
			}
			sawSubType = true
			d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			err = d.readQuoteStart()
			if err != nil {
				return nil, err
			}
			out, err = d.convertBinarySubType(out, subTypeBytePos)
			if err != nil {
				return nil, err
			}
			if !sawBase64 {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Compare(key, jsonBase64) == 0:
			if sawBase64 {
				return nil, d.parseError(key[0], "base64 repeated")
			}
			sawBase64 = true
			d.json.Discard(len(key) + 1)
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
			if !sawSubType {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(key[0], "invalid key for $binary document")
		}
		if sawBase64 && sawSubType {
			break
		}
	}

	// write length of binary payload (added length minux 5 bytes for
	// length+type)
	binLength := len(out) - lengthPos - 5
	overwriteLength(out, lengthPos, binLength)

	// Must end with document terminator
	err := d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

// v1 $binary is a string, followed by "$type" and no other keys.  This function
// is called after the opening quote of the base64 payload is already read.
func (d *Decoder) convertV1Binary(out []byte) ([]byte, error) {
	// write a length placeholder and a type placeholder
	lengthPos := len(out)
	out = append(out, emptyLength...)
	subTypeBytePos := len(out)
	out = append(out, emptyType)

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
	key, err := d.peekBoundedQuote(6, 6)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(key, jsonType) != 0 {
		d.parseError(key[0], "expected $type")
	}
	d.json.Discard(len(key) + 1)
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	out, err = d.convertBinarySubType(out, subTypeBytePos)
	if err != nil {
		return nil, err
	}

	// write length of binary payload (added length minux 5 bytes for
	// length+type)
	binLength := len(out) - lengthPos - 5
	overwriteLength(out, lengthPos, binLength)

	return out, nil
}

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

var dollarOptionsExtJSONRe = regexp.MustCompile(`^\$options"\s*:\s*"[a-z]*"\s*,\s*"\$regex"\s*:\s*"`)
var dollarOptionsQueryOpRe = regexp.MustCompile(`^\$options"\s*:\s*"[a-z]*"\s*,\s*"\$regex"\s*:\s*\{`)

func (d *Decoder) convertOptions(out []byte, typeBytePos int) ([]byte, error) {
	// Peek ahead successively longer; shouldn't be necessary but
	// covers a pathological case with excessive white space
	peekDistance := 64
	var isExtJSON bool
	var err error
	var buf []byte
	for peekDistance < d.json.Size() {
		buf, err = d.json.Peek(peekDistance)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
		}
		// Smallest possible matching buffer is 23 chars: `$options":"","$regex":{`
		if len(buf) < 23 {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}
		if dollarOptionsExtJSONRe.Match(buf) {
			isExtJSON = true
			break
		} else if dollarOptionsQueryOpRe.Match(buf) {
			// Signal not extended JSON with double nil.
			return nil, nil
		}
		if len(buf) < peekDistance {
			break
		}
		peekDistance *= 2
	}
	if !isExtJSON {
		return nil, fmt.Errorf("parse error: invalid $regex Extended JSON at %q", string(buf))
	}

	overwriteTypeByte(out, typeBytePos, bsonRegex)

	// Discard $options key and closing quote
	d.json.Discard(9)
	// Read name separator and opening quote
	err = d.readNameSeparator()
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	// Read options string
	opts := make([]byte, 0, 8)
	opts, err = d.convertCString(opts)
	if err != nil {
		return nil, err
	}

	// $regex key must be next
	err = d.readCharAfterWS(',')
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	err = d.readSpecificKey(jsonRegex)
	if err != nil {
		return nil, err
	}
	err = d.readQuoteStart()
	if err != nil {
		return nil, err
	}
	out, err = d.convertCString(out)
	if err != nil {
		return nil, err
	}
	// Append options
	out = append(out, opts...)

	// Must end with document terminator.
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
		// Read key and skip ahead to start of value.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		key, err := d.peekBoundedQuote(4, 5)
		if err != nil {
			return nil, newReadError(err)
		}

		// Handle the key.
		switch {
		case bytes.Compare(key, jsonRef) == 0:
			if sawRef {
				return nil, d.parseError(key[0], "key '$ref' repeated")
			}
			sawRef = true
			d.json.Discard(len(key) + 1)
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

			if !sawID {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Compare(key, jsonID) == 0:
			if sawID {
				return nil, d.parseError(key[0], "key '$id' repeated")
			}
			sawID = true
			d.json.Discard(len(key) + 1)
			err = d.readNameSeparator()
			if err != nil {
				return nil, err
			}
			// Value must be of type object ID.  Read the value into a temporary
			// buffer, reserving the first byte for discovered type.
			id = make([]byte, 1, 13)
			id, err = d.convertValue(id, 0)
			if err != nil {
				return nil, err
			}
			if id[0] != bsonObjectID {
				return nil, fmt.Errorf("parse error: $dbPointer.$id must be BSON type %d, not type %d", bsonObjectID, id[0])
			}
			if !sawRef {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(key[0], "invalid key for $regularExpression document")
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
	buf, err := d.peekBoundedQuote(2, 12)
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(buf), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parser error: int conversion: %v", err)
	}
	var x [4]byte
	xs := x[0:4]
	binary.LittleEndian.PutUint32(xs, uint32(n))
	out = append(out, xs...)

	// Discard buffer and trailing quote
	d.json.Discard(len(buf) + 1)

	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
		// Read key and skip ahead to start of value.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		ch, err := d.json.ReadByte()
		if err != nil {
			return nil, newReadError(err)
		}
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

		// Handle the key.
		switch ch {
		case 't':
			if sawT {
				return nil, d.parseError(ch, "key 't' repeated")
			}
			sawT = true
			timestamp, err = d.readUInt32()
			if err != nil {
				return nil, err
			}
			if !sawI {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case 'i':
			if sawI {
				return nil, d.parseError(ch, "key 'i' repeated")
			}
			sawI = true
			increment, err = d.readUInt32()
			if err != nil {
				return nil, err
			}
			if !sawT {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(ch, "invalid key for $timestamp document")
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

	// Must end with document terminator
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
	if bytes.Compare(buf, []byte{'r', 'u', 'e'}) != 0 {
		return nil, d.parseError('t', "expected 'true'")
	}

	d.json.Discard(3)
	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
	buf, err := d.peekBoundedQuote(2, 21)
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parser error: int conversion: %v", err)
	}
	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, uint64(n))
	out = append(out, xs...)

	// Discard buffer and trailing quote
	d.json.Discard(len(buf) + 1)

	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
	buf, err := d.peekBoundedQuote(2, doublePeekWidth)
	if err != nil {
		return nil, err
	}

	n, err := strconv.ParseFloat(string(buf), 64)
	if err != nil {
		return nil, fmt.Errorf("parser error: float conversion: %v", err)
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
	d.json.Discard(len(buf) + 1)

	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
	buf, err := d.peekBoundedQuote(2, decimalPeekWidth)
	if err != nil {
		return nil, err
	}

	d128, err := primitive.ParseDecimal128(string(buf))
	if err != nil {
		return nil, fmt.Errorf("parser error: decimal128 conversion: %v", err)
	}

	hi, lo := d128.GetBytes()
	var x [8]byte
	xs := x[0:8]
	binary.LittleEndian.PutUint64(xs, lo)
	out = append(out, xs...)
	binary.LittleEndian.PutUint64(xs, hi)
	out = append(out, xs...)

	// Discard buffer and trailing quote
	d.json.Discard(len(buf) + 1)

	err = d.readObjectTerminator()
	if err != nil {
		return nil, err
	}

	return out, nil
}

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
		// Read key and skip ahead to start of value.
		err := d.readQuoteStart()
		if err != nil {
			return nil, err
		}
		key, err := d.peekBoundedQuote(8, 8)
		if err != nil {
			return nil, newReadError(err)
		}

		// Handle the key.
		switch {
		case bytes.Compare(key, jsonREpattern) == 0:
			if sawPattern {
				return nil, d.parseError(key[0], "key 'pattern' repeated")
			}
			sawPattern = true
			d.json.Discard(len(key))
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

			if !sawOptions {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		case bytes.Compare(key, jsonREoptions) == 0:
			if sawOptions {
				return nil, d.parseError(key[0], "key 'options' repeated")
			}
			sawOptions = true
			d.json.Discard(len(key))
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

			options = make([]byte, 0, 256)
			options, err = d.convertCString(options)
			if err != nil {
				return nil, err
			}

			if !sawPattern {
				err = d.readCharAfterWS(',')
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, d.parseError(key[0], "invalid key for $regularExpression document")
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

// starts after opening quote mark
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

		// If we have characters, decode and append them
		if len(buf) > 0 {
			n, err := enc.Decode(xs, buf)
			if err != nil {
				return nil, d.parseError(buf[0], fmt.Sprintf("error parsing base64 data: %s", err))
			}
			out = append(out, xs[0:n]...)
			d.json.Discard(len(buf))
		}

		// If terminated, discard closing quote
		if terminated {
			d.json.Discard(1)
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
		return 0, fmt.Errorf("invalid $date value string: %s", string(data))
	}

	return t.Unix()*1e3 + int64(t.Nanosecond())/1e6, nil
}
