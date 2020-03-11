// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// ErrUnsupportedBOM means that a UTF-16 or UTF-32 byte order mark was found.
var ErrUnsupportedBOM = errors.New("unsupported byte order mark")

// Decoder reads and decodes JSON objects to BSON from a buffered input stream.
// Objects may be separated by optional white space or may be in a well-formed
// JSON array at the top-level.
type Decoder struct {
	arrayFinished  bool
	arrayStarted   bool
	curDepth       int
	extJSONAllowed bool
	json           *bufio.Reader
	maxDepth       int
	scratchPool    *sync.Pool
}

// NewDecoder returns a new decoder.  If a UTF-8 byte-order-mark (BOM) exists,
// it will be stripped.  Because only UTF-8 is supported, other BOMs are error
// and will return ErrUnsupportedBOM.  This function consumes leading white
// space and checks if the first character is '['.  If so, the input format is
// expected to be a single JSON array of objects and the stream will consist of
// the objects in the array.  Any read error (including io.EOF) during these
// checks will be returned.
//
// If the the bufio.Reader's size is less than 8192, it will be rebuffered.
// This is necessary to account for lookahead for long decimals to minimize
// copying.
func NewDecoder(json *bufio.Reader) (*Decoder, error) {
	if json.Size() < 8192 {
		json = bufio.NewReaderSize(json, 8192)
	}
	err := handleBOM(json)
	if err != nil {
		return nil, err
	}

	d := &Decoder{
		json:        json,
		maxDepth:    200,
		scratchPool: &sync.Pool{New: func() interface{} { return make([]byte, 0, 256) }},
	}

	ch, err := d.readAfterWS()
	if err != nil {
		// Before an object is read, EOF is a valid response that
		// shouldn't be wrapped.
		if err == io.EOF {
			return nil, err
		}
		return nil, newReadError(err)
	}

	switch ch {
	case '[':
		d.arrayStarted = true
	default:
		_ = d.json.UnreadByte()
	}

	return d, nil
}

// ExtJSON toggles whether extended JSON is interpreted by the decoder.
// See https://docs.mongodb.com/manual/reference/mongodb-extended-json/index.html
// Jibby has limited support for the legacy extended JSON format.
func (d *Decoder) ExtJSON(b bool) {
	d.extJSONAllowed = b
}

// MaxDepth sets the maximum allowed depth of a JSON object.  The default is
// 200.
func (d *Decoder) MaxDepth(n int) {
	d.maxDepth = n
}

// Decode converts a single JSON object from the input stream into BSON object.
// The function takes an output buffer as an argument.  If the buffer is not
// large enough, a new buffer will be allocated when needed.  The final buffer
// is returned, just like with `append`.  The function returns io.EOF if no
// objects remain in the stream.
func (d *Decoder) Decode(buf []byte) ([]byte, error) {
	if d.arrayFinished {
		return nil, io.EOF
	}

	ch, err := d.readAfterWS()
	if err != nil {
		// Before an object is read, EOF is a valid response that
		// shouldn't be wrapped.
		if err == io.EOF {
			return nil, err
		}
		return nil, newReadError(err)
	}

	switch ch {
	case '{':
		_ = d.json.UnreadByte()
	case ']':
		// This case will only occur for an empty top-level array: `[]`.
		// Otherwise, the closing array bracket is read after an object.
		if d.arrayStarted {
			d.arrayFinished = true
			return nil, io.EOF
		}
		return nil, d.parseError([]byte{ch}, "Decode only supports object decoding")
	default:
		return nil, d.parseError([]byte{ch}, "Decode only supports object decoding")
	}

	buf, err = d.convertValue(buf, topContainer)
	if err != nil {
		return nil, err
	}

	// If in comma mode, consume comma or ']', otherwise, put the
	// the character back to be
	// After ']', terminate stream?
	if d.arrayStarted {
		ch, err := d.readAfterWS()
		if err != nil {
			return nil, newReadError(err)
		}

		switch ch {
		case ',':
			// nothing
		case ']':
			d.arrayFinished = true
		default:
			return nil, d.parseError([]byte{ch}, "expecting value-separator or end of array")
		}
	}

	return buf, nil
}

// readAfterWS discards JSON white space and returns the next character.
// Any error that occurs is returned without wrapping.
func (d *Decoder) readAfterWS() (byte, error) {
	var ch byte
	var err error
	for {
		ch, err = d.json.ReadByte()
		if err != nil {
			// Don't use newReadError here as we don't know if there must be
			// another character.  Let the caller decide.
			return 0, err
		}
		switch ch {
		case ' ', '\t', '\n', '\r':
		default:
			return ch, nil
		}
	}
}

// skipWS consumes white space but leaves the next character in the input
// stream.  Any error that occurs is returned without wrapping.
func (d *Decoder) skipWS() error {
	_, err := d.readAfterWS()
	if err != nil {
		return err
	}
	_ = d.json.UnreadByte()
	return nil
}

// readCharAfterWS reads a specific character after white space or errors
// if the character is not available.  Any read error is returned, with
// EOF upgraded to io.ErrUnexpectedEOF.
func (d *Decoder) readCharAfterWS(b byte) error {
	ch, err := d.readAfterWS()
	if err != nil {
		return newReadError(err)
	}
	if ch != b {
		return d.parseError([]byte{ch}, fmt.Sprintf("expecting '%c'", b))
	}
	return nil
}

// readNextChar expects a specific character to be next in the input stream and
// errors otherwise.  Any read error is returned, with EOF upgraded to
// io.ErrUnexpectedEOF.
func (d *Decoder) readNextChar(b byte) error {
	ch, err := d.json.ReadByte()
	if err != nil {
		return newReadError(err)
	}
	if ch != b {
		return d.parseError([]byte{ch}, fmt.Sprintf("expecting '%c'", b))
	}
	return nil
}

// readNameSeparator expects the ':' character after optional white space and
// errors if it not found.  It handles other errors like readCharAfterWS.
func (d *Decoder) readNameSeparator() error {
	err := d.readCharAfterWS(':')
	if err != nil {
		return err
	}
	return nil
}

// readNameSeparator expects the '}' character after optional white space and
// errors if it is not found.  It handles other errors like readCharAfterWS.
func (d *Decoder) readObjectTerminator() error {
	err := d.readCharAfterWS('}')
	if err != nil {
		return err
	}
	return nil
}

// readNameSeparator expects the '"' character after optional white space and
// errors if it is not found.  It handles other errors like readCharAfterWS.
func (d *Decoder) readQuoteStart() error {
	err := d.readCharAfterWS('"')
	if err != nil {
		return err
	}
	return nil
}

// readSpecificKey expects and consumes a specific series of bytes representing
// an object key, and errors if it is not found.  The starting quote character
// must have already been consumed from the input stream.  The closing quote and
// subsequent name separator will also be consumed from the input stream.  It
// handles other errors like readCharAfterWS.
func (d *Decoder) readSpecificKey(expected []byte) error {
	charsNeeded := len(expected) + 1
	key, err := d.peekBoundedQuote(charsNeeded, charsNeeded, string(expected))
	if err != nil {
		return err
	}
	if !bytes.Equal(key, expected) {
		if len(key) > 0 {
			return d.parseError(nil, fmt.Sprintf("expected %q", string(expected)))
		}
		return d.parseError(nil, fmt.Sprintf("expected %q", string(expected)))
	}
	_, _ = d.json.Discard(len(key) + 1)
	err = d.readNameSeparator()
	if err != nil {
		return err
	}
	return nil
}

// peekNumber peeks into the input stream and returns a slice that might be
// parsable as a number, a boolean hint whether it should be treated as a
// floating point number, and any error that is found.  The input stream is
// not consumed.
//
// The slice returned will contain characters that could appear in a JSON
// floating point number (excluding "NaN" and "Inf", which are not legal JSON)
// without regard to legal arrangment.  For example, `123-eee` is a possible
// return value.  This function only only parses out a string to be passed to a
// numeric conversion function.
func (d *Decoder) peekNumber() ([]byte, bool, error) {
	var isFloat bool
	var terminated bool

	buf, err := d.json.Peek(doublePeekWidth)
	if err != nil {
		// here, io.EOF is OK, since we're peeking and may hit end of
		// object within the peek width.
		if err != io.EOF {
			return nil, false, newReadError(err)
		}
	}

	// Find where the number appears to ends and if it's a float.  A
	// number ends at white space, a separator, or a terminator.
	var i int
LOOP:
	for i = 0; i < len(buf); i++ {
		switch buf[i] {
		case 'e', 'E':
			isFloat = true
		case '.':
			isFloat = true
			if i < len(buf)-1 && (buf[i+1] < '0' || buf[i+1] > '9') {
				_, _ = d.json.Discard(i)
				return nil, false, d.parseError(nil, "decimal must be followed by digit")
			}
		case ' ', '\t', '\n', '\r', ',', ']', '}':
			terminated = true
			break LOOP
		case '_':
			_, _ = d.json.Discard(i)
			return nil, false, d.parseError(nil, "invalid character in number")
		}
	}

	if !terminated {
		if len(buf) < doublePeekWidth {
			return nil, false, newReadError(io.ErrUnexpectedEOF)
		}
		return nil, false, d.parseError(nil, "number too long")
	}

	// Do some validation before ParseInt/ParseFloat for basic sanity and for
	// things that ParseInt/ParseFloat are liberal about.
	num := buf[0:i]

	// Check for optional leading minus; skip it for other validation
	if len(num) > 1 && num[0] == '-' {
		num = num[1:]
	}

	// Check for empty string
	if len(num) == 0 {
		return nil, false, d.parseError(nil, "number not found")
	}

	// Check for number
	if num[0] < '0' || num[0] > '9' {
		return nil, false, d.parseError(nil, "invalid character in number")
	}

	if num[0] == '0' && len(num) > 1 && num[1] != '.' && num[1] != 'e' && num[1] != 'E' {
		return nil, false, d.parseError(nil, "leading zeros not allowed")
	}

	// Return the slice without the terminating character.
	return buf[0:i], isFloat, nil
}

// peekUint32 works like like peekNumber but only for characters valid
// for a Uint32.
func (d *Decoder) peekUint32() ([]byte, error) {
	buf, err := d.peekInt(uint32PeekWidth)
	if err != nil {
		return nil, err
	}
	if buf[0] == '-' {
		return nil, d.parseError(nil, "negative number not allowed")
	}
	return buf, nil
}

// peekInt64 works like like peekNumber but only for characters valid
// for a Int64.
func (d *Decoder) peekInt64() ([]byte, error) {
	buf, err := d.peekInt(int64PeekWidth)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (d *Decoder) peekInt(intWidth int) ([]byte, error) {
	var terminated bool

	buf, err := d.json.Peek(intWidth)
	if err != nil {
		// here, io.EOF is OK, since we're peeking and may hit end of
		// object
		if err != io.EOF {
			return nil, newReadError(err)
		}
	}

	// Find where the number appears to ends.
	var i int
LOOP:
	for i = 0; i < len(buf); i++ {
		switch buf[i] {
		case ' ', '\t', '\n', '\r', ',', ']', '}':
			terminated = true
			break LOOP
		}
	}

	// Do some validation before ParseInt/ParseFloat for basic sanity and for
	// things that ParseInt/ParseFloat are liberal about.
	num := buf[0:i]

	// Check for empty string
	if len(num) == 0 {
		return nil, d.parseError(nil, "number not found")
	}

	// Remove a leading negative sign, if any, before further validation.
	if num[0] == '-' {
		num = num[1:]
		if len(num) == 0 {
			return nil, d.parseError(nil, "number not found")
		}
	}

	// Check for number
	if num[0] < '0' || num[0] > '9' {
		return nil, d.parseError(nil, "invalid character in number")
	}

	if num[0] == '0' && len(num) > 1 {
		return nil, d.parseError(nil, "leading zeros not allowed")
	}

	if !terminated {
		if len(buf) < intWidth {
			return nil, newReadError(io.ErrUnexpectedEOF)
		}
		return nil, d.parseError(nil, "number too long")
	}

	return buf[0:i], nil
}

// readUint32 consumes a Uint32 value from the input stream or an error
// if the input stream doesn't begin with a Uint32 value.
func (d *Decoder) readUint32() (uint32, error) {
	buf, err := d.peekUint32()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(string(buf), 10, 32)
	if err != nil {
		return 0, d.parseError(nil, fmt.Sprintf("uint conversion: %v", err))
	}
	_, _ = d.json.Discard(len(buf))
	return uint32(n), nil
}

// readInt64 consumes a Int64 value from the input stream or an error
// if the input stream doesn't begin with an Int64 value.
func (d *Decoder) readInt64() (int64, error) {
	buf, err := d.peekInt64()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return 0, d.parseError(nil, fmt.Sprintf("int conversion: %v", err))
	}
	_, _ = d.json.Discard(len(buf))
	return int64(n), nil
}

// peekBoundedQuote peeks into the input stream for a series of non-quote
// characters terminated by a closing quote.  The function takes a minimum and
// maximum length (including closing quote) and errors if it can't find a
// sequence plus quote within those boundaries.  The byte slice returned
// *excludes* the closing quote.  Nothing is consumed from the input stream.
//
// NOTE: JSON string escapes (`\n`, etc.) are not supported/interpreted.
func (d *Decoder) peekBoundedQuote(minLen, maxLen int, label string) ([]byte, error) {
	buf, err := d.json.Peek(maxLen)
	if err != nil {
		if err != io.EOF {
			return nil, newReadError(err)
		}
	}

	if len(buf) < minLen {
		return nil, newReadError(io.ErrUnexpectedEOF)
	}

	quotePos := bytes.IndexByte(buf, '"')
	if quotePos < 0 {
		return nil, d.parseError(nil, fmt.Sprintf("string exceeds expected maximum length %d for %s", maxLen-1, label))
	} else if quotePos < minLen-1 {
		return nil, d.parseError(nil, fmt.Sprintf("string shorter than expected minimum length %d for %s", minLen-1, label))
	}

	return buf[0:quotePos], nil
}

const parseErrorContextLength = 10

// parseError returns an error with a message and some context for where it occurs.
func (d *Decoder) parseError(startingAt []byte, msg string) error {
	if len(startingAt) < parseErrorContextLength {
		after, err := d.json.Peek(parseErrorContextLength - len(startingAt))
		if len(after) > 0 {
			startingAt = append(startingAt, after...)
		}
		if err == nil {
			// Add ellipsis to show there is more
			startingAt = append(startingAt, '.', '.', '.')
		}
	}
	if len(startingAt) > 0 {
		return fmt.Errorf("parse error at `%s`: %s", startingAt, msg)
	}
	return fmt.Errorf("parse error: %s", msg)
}

// copyPeek returns a copy of a Peek into the start of the buffer.
func (d *Decoder) copyPeek(length int) []byte {
	buf, _ := d.json.Peek(length)
	if len(buf) == 0 {
		return nil
	}
	out := make([]byte, len(buf))
	copy(out, buf)
	return out
}

// Unmarshal converts a single JSON object to a BSON document.  The function
// takes an output buffer as an argument.  If the buffer is not large enough, a
// new buffer will be allocated on demand.  The final buffer is returned, just
// like with `append`.  The function returns io.EOF if the input is empty.
func Unmarshal(in []byte, out []byte) ([]byte, error) {
	jsonReader := bufio.NewReaderSize(bytes.NewReader([]byte(in)), 8192)
	jib, err := NewDecoder(jsonReader)
	if err != nil {
		return nil, err
	}
	return jib.Decode(out)
}

// UnmarshalExtJSON converts a single Extended JSON object to a BSON document.
// It otherwise works like `Unmarshal`.
func UnmarshalExtJSON(in []byte, out []byte) ([]byte, error) {
	jsonReader := bufio.NewReaderSize(bytes.NewReader([]byte(in)), 8192)
	jib, err := NewDecoder(jsonReader)
	if err != nil {
		return nil, err
	}
	jib.ExtJSON(true)
	return jib.Decode(out)
}

// overwriteTypeByte is a helper for writing a type byte that
// no-ops for a top-level container.
func overwriteTypeByte(out []byte, pos int, bsonType byte) {
	// Top-level containers don't have a type byte preceding them
	if pos == topContainer {
		return
	}
	out[pos] = bsonType
}

// overwriteLength is a readability helper to write a length to a a particular buffer
// location as little-endian int32.
func overwriteLength(out []byte, pos int, n int) {
	binary.LittleEndian.PutUint32(out[pos:pos+4], uint32(n))
}

// handleBOM will detect/discard/error based on the BOM. Inability to peek a BOM is a
// no-op, not an error so it can be handled by the normal parser.  Only UTF-8
// BOM is supported; others will error.
func handleBOM(r *bufio.Reader) error {
	// Peek 2 byte BOMs
	preamble, err := r.Peek(2)
	if err != nil {
		return nil
	}
	if bytes.Equal(preamble, utf16BEBOM) || bytes.Equal(preamble, utf16LEBOM) {
		return ErrUnsupportedBOM
	}

	// Peek 3 byte BOM; UTF-8 is supported, so discard them if found.
	preamble, err = r.Peek(3)
	if err != nil {
		return nil
	}
	if bytes.Equal(preamble, utf8BOM) {
		_, _ = r.Discard(3)
	}

	// Peek 4 byte BOMs
	preamble, err = r.Peek(4)
	if err != nil {
		return nil
	}
	if bytes.Equal(preamble, utf32BEBOM) || bytes.Equal(preamble, utf32LEBOM) {
		return ErrUnsupportedBOM
	}

	return nil
}

// newReadError is used when we expect to be able to read and fail.  If the
// error is EOF, we convert it to UnexpectedEOF because we aren't between
// top-level object.
func newReadError(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("read error while parsing: %w", err)
}
