// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package jibby is a high-performance, streaming JSON-to-BSON decoder.  It
// decodes successive JSON objects into BSON documents from a buffered input
// byte stream while minimizing memory copies.  Only UTF-8 encoding is supported
// and input text is expected to be well-formed.
//
// Extended JSON is not yet supported.
package jibby

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Decoder reads and decodes JSON objects to BSON from a buffered input stream.
// Objects may be separated by optional white space or may be in a well-formed
// JSON array.
type Decoder struct {
	arrayFinished  bool
	arrayStarted   bool
	curDepth       int
	extJSONAllowed bool
	json           *bufio.Reader
	maxDepth       int
}

// NewDecoder returns a new decoder.  If a UTF-8 byte-order-mark (BOM) exists,
// it will be stripped.  Because only UTF-8 is supported, other BOMs are errors.
// This function consumes leading white space and checks if the first character
// is '['.  If so, the input format is expected to be a single JSON array of
// objects and the stream will consist of the objects in the array.  Any read
// error (including io.EOF) will be returned.
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
		json:     json,
		maxDepth: 200,
	}

	ch, err := d.readAfterWS()
	if err != nil {
		// Before an object is read, EOF is valid.
		if err == io.EOF {
			return nil, err
		}
		return nil, newReadError(err)
	}

	switch ch {
	case '[':
		d.arrayStarted = true
	default:
		err = d.json.UnreadByte()
		if err != nil {
			return nil, err
		}
	}

	return d, err
}

// ExtJSON toggles whether extended JSON is interpreted by the decoder.
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
// large enough, a new buffer will be allocated on demand.  The final buffer is
// returned, just like with `append`.  The function returns io.EOF if no
// objects remain in the stream.
func (d *Decoder) Decode(buf []byte) ([]byte, error) {
	if d.arrayFinished {
		return nil, io.EOF
	}

	ch, err := d.readAfterWS()
	if err != nil {
		// Before reading a new object, EOF is valid.
		if err == io.EOF {
			return nil, err
		}
		return nil, newReadError(err)
	}

	switch ch {
	case '{':
		err = d.json.UnreadByte()
		if err != nil {
			return nil, err
		}
	case ']':
		if d.arrayStarted {
			d.arrayFinished = true
			return nil, io.EOF
		}
		return nil, d.parseError(ch, "Decode only supports object decoding")
	default:
		return nil, d.parseError(ch, "Decode only supports object decoding")
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
			return nil, d.parseError(ch, "expecting value-separator or end of array")
		}
	}

	return buf, nil
}

func (d *Decoder) readAfterWS() (byte, error) {
	var ch byte
	var err error
	for {
		ch, err = d.json.ReadByte()
		if err != nil {
			return 0, err
		}
		switch ch {
		case ' ', '\t', '\n', '\r':
		default:
			return ch, nil
		}
	}
}

func (d *Decoder) readCharAfterWS(b byte) error {
	ch, err := d.readAfterWS()
	if err != nil {
		return newReadError(err)
	}
	if ch != b {
		return d.parseError(ch, fmt.Sprintf("expecting '%c'", b))
	}
	return nil
}

func (d *Decoder) readNameSeparator() error {
	err := d.readCharAfterWS(':')
	if err != nil {
		return err
	}
	return nil
}

func (d *Decoder) readObjectTerminator() error {
	err := d.readCharAfterWS('}')
	if err != nil {
		return err
	}
	return nil
}

func (d *Decoder) readQuoteStart() error {
	err := d.readCharAfterWS('"')
	if err != nil {
		return err
	}
	return nil
}

func (d *Decoder) readSpecificKey(expected []byte) error {
	charsNeeded := len(expected) + 1
	key, err := d.peekBoundedQuote(charsNeeded, charsNeeded)
	if err != nil {
		return err
	}
	if bytes.Compare(key, expected) != 0 {
		d.parseError(key[0], fmt.Sprintf("expected %q", string(expected)))
	}
	d.json.Discard(len(key) + 1)
	err = d.readNameSeparator()
	if err != nil {
		return err
	}
	return nil
}

func (d *Decoder) peekBoundedQuote(minLen, maxLen int) ([]byte, error) {
	buf, err := d.json.Peek(maxLen)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	if len(buf) < minLen {
		return nil, newReadError(io.ErrUnexpectedEOF)
	}

	quotePos := bytes.IndexByte(buf, '"')
	if quotePos < 0 {
		return nil, d.parseError(buf[len(buf)-1], "string not terminated within expected length")
	}

	return buf[0:quotePos], nil
}

func (d *Decoder) parseError(ch byte, msg string) error {
	after, _ := d.json.Peek(20)
	return fmt.Errorf("parse error: %s on char '%s', followed by '%s...'", msg, string(ch), after)
}

// Unmarshal converts a single JSON object to a BSON document.  The function
// takes an output buffer as an argument.  If the buffer is not large enough, a
// new buffer will be allocated on demand.  The final buffer is returned, just
// like with `append`.  The function returns io.EOF if the input is empty.
func Unmarshal(in []byte, out []byte) ([]byte, error) {
	jsonReader := bufio.NewReader(bytes.NewReader([]byte(in)))
	jib, err := NewDecoder(jsonReader)
	if err != nil {
		return nil, err
	}
	return jib.Decode(out)
}

// UnmarshalExtJSON converts a single Extended JSON object to a BSON document.
// It otherwise works like `Unmarshal`.
func UnmarshalExtJSON(in []byte, out []byte) ([]byte, error) {
	jsonReader := bufio.NewReader(bytes.NewReader([]byte(in)))
	jib, err := NewDecoder(jsonReader)
	jib.ExtJSON(true)
	if err != nil {
		return nil, err
	}
	return jib.Decode(out)
}

func overwriteTypeByte(out []byte, pos int, bsonType byte) {
	// Top-level containers don't have a type byte preceding them
	if pos == topContainer {
		return
	}
	out[pos] = bsonType
}

func overwriteLength(out []byte, pos int, n int) {
	binary.LittleEndian.PutUint32(out[pos:pos+4], uint32(n))
}

// detect/discard/error on BOM. Inability to peek is a NOP and
// will be handled by the normal parser
func handleBOM(r *bufio.Reader) error {
	// Peek 2 byte BOMs
	preamble, err := r.Peek(2)
	if err != nil {
		return nil
	}
	if bytes.Compare(preamble, utf16BEBOM) == 0 || bytes.Compare(preamble, utf16LEBOM) == 0 {
		return fmt.Errorf("error: detected unsupported UTF-16 BOM")
	}

	// Peek 3 byte BOM; UTF-8 is supported, so discard them if found.
	preamble, err = r.Peek(3)
	if err != nil {
		return nil
	}
	if bytes.Compare(preamble, utf8BOM) == 0 {
		_, _ = r.Discard(3)
	}

	// Peek 4 byte BOMs
	preamble, err = r.Peek(4)
	if err != nil {
		return nil
	}
	if bytes.Compare(preamble, utf32BEBOM) == 0 || bytes.Compare(preamble, utf32LEBOM) == 0 {
		return fmt.Errorf("error: detected unsupported UTF-32 BOM")
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
	return fmt.Errorf("error reading json: %w", err)
}
