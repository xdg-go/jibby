// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

const JSONTestSuite = "testdata/JSONTestSuite/test_parsing"
const JibbyTestSuite = "testdata/jibbytests"

// TestJSONTestSuite_Passing tests the seriot.ch corpus for
// cases that must pass.
func TestJSONTestSuite_Passing(t *testing.T) {
	t.Helper()
	t.Parallel()
	files := getTestFiles(t, JSONTestSuite, "y", ".json")
	for _, f := range files {
		path := filepath.Join(JSONTestSuite, f)
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, path, nil)
		})
	}
}

// TestJibbyTestSuite_Passing tests a local corpus in the
// same format as the seriot.ch corpus that must pass.
func TestJibbyTestSuite_Passing(t *testing.T) {
	t.Helper()
	t.Parallel()
	files := getTestFiles(t, JibbyTestSuite, "y", ".json")
	for _, f := range files {
		path := filepath.Join(JibbyTestSuite, f)
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, path, nil)
		})
	}
}

// For implementation defined behavior: these errors are allowed as we don't
// support the related features.
var allowedErrors = []string{
	ErrUnsupportedBOM.Error(),
	"value out of range", // large ints/floats
}

// We assume valid UTF-8; these files test handling invalid strings,
// so we skip these files.
var unsupportedTests = map[string]bool{
	"i_string_UTF-8_invalid_sequence.json":         true,
	"i_string_UTF8_surrogate_U+D800.json":          true,
	"i_string_invalid_utf-8.json":                  true,
	"i_string_iso_latin_1.json":                    true,
	"i_string_lone_utf8_continuation_byte.json":    true,
	"i_string_not_in_unicode_range.json":           true,
	"i_string_overlong_sequence_2_bytes.json":      true,
	"i_string_overlong_sequence_6_bytes.json":      true,
	"i_string_overlong_sequence_6_bytes_null.json": true,
	"i_string_truncated-utf-8.json":                true,
	"i_string_utf16BE_no_BOM.json":                 true,
	"i_string_utf16LE_no_BOM.json":                 true,
}

// TestJSONTestSuite_Passing tests the seriot.ch corpus for cases that are
// implementation defined.  Variables above define exclusions.
func TestJSONTestSuite_ImplDefined(t *testing.T) {
	t.Helper()
	t.Parallel()

	files := getTestFiles(t, JSONTestSuite, "i", ".json")
	for _, f := range files {
		if unsupportedTests[f] {
			continue
		}
		path := filepath.Join(JSONTestSuite, f)
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, path, allowedErrors)
		})
	}
}

// GODRIVER-1947: The MongoDB Go driver doesn't properly handle surrogate pairs
// yet, so we allow different results for these.
var godriver1947 = map[string]bool{
	"y_string_unicode_U+1FFFE_nonchar.json":                  true,
	"y_string_unicode_U+10FFFE_nonchar.json":                 true,
	"y_string_surrogates_U+1D11E_MUSICAL_SYMBOL_G_CLEF.json": true,
	"y_string_last_surrogates_1_and_2.json":                  true,
	"y_string_accepted_surrogate_pair.json":                  true,
	"y_string_accepted_surrogate_pairs.json":                 true,
	"i_string_inverted_surrogates_U+1D11E.json":              true,
	"i_string_incomplete_surrogates_escape_valid.json":       true,
}

func testPassingConversion(t *testing.T, f string, allowedErrStrings []string) {
	t.Helper()
	text, err := ioutil.ReadFile(f)
	if err != nil {
		t.Fatalf("error reading %s: %v", f, err)
	}
	text = objectify(text)
	jibbyGot, err := convertWithJibby(text)
	if err != nil {
		for _, v := range allowedErrStrings {
			if strings.Contains(err.Error(), v) {
				return
			}
		}
		t.Fatalf("jibby error: %v\ntext: %s", err, string(text))
	}
	driverGot, err := convertWithGoDriver(text)
	if err != nil {
		// If Go driver can't parse, we can't compare against it.
		t.Logf("skipping, mongo go driver error: %v\ntext: %s", err, string(text))
		return
	}
	if !bytes.Equal(jibbyGot, driverGot) {
		baseName := filepath.Base(f)
		if godriver1947[baseName] {
			return
		}
		t.Fatalf("jibby doesn't match Go driver:\njibby:  %v\nDriver: %v", hex.EncodeToString(jibbyGot), hex.EncodeToString(driverGot))
	}
}

// TestJSONTestSuite_Failing tests the seriot.ch corpus for
// cases that must error.
func TestJSONTestSuite_Failing(t *testing.T) {
	t.Parallel()

	files := getTestFiles(t, JSONTestSuite, "n", ".json")
	for _, f := range files {
		path := filepath.Join(JSONTestSuite, f)
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testFailingConversion(t, path)
		})
	}
}

// TestJibbyTestSuite_Passing tests a local corpus in the
// same format as the seriot.ch corpus that must error.
func TestJibbyTestSuite_Failing(t *testing.T) {
	t.Parallel()

	files := getTestFiles(t, JibbyTestSuite, "n", ".json")
	for _, f := range files {
		path := filepath.Join(JibbyTestSuite, f)
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testFailingConversion(t, path)
		})
	}
}

func testFailingConversion(t *testing.T, f string) {
	t.Helper()
	var err error
	var jibbyGot []byte
	text, err := ioutil.ReadFile(f)
	if err != nil {
		t.Fatalf("error reading %s: %v", f, err)
	}
	jsonReader := bufio.NewReader(bytes.NewReader(text))
	jib, err := NewDecoder(jsonReader)
	for err == nil {
		jibbyGot, err = jib.Decode(make([]byte, 0, 256))
	}
	if err == nil {
		t.Fatalf("expected error but got none for '%s' ('%s')", string(text), hex.EncodeToString(jibbyGot))
	}
}

// TestStreaming has tests for document streams, including different formats,
// empty lists, error cases, etc.
func TestStreaming(t *testing.T) {
	t.Parallel()

	type testCase struct {
		label  string
		input  string
		count  int
		errStr string
	}

	cases := []testCase{
		// Document streams
		{
			label:  "no docs",
			input:  "",
			count:  0,
			errStr: io.EOF.Error(),
		},
		{
			label:  "1 doc",
			input:  "{}",
			count:  1,
			errStr: io.EOF.Error(),
		},
		{
			label:  "1 doc, leading WS",
			input:  " {}",
			count:  1,
			errStr: io.EOF.Error(),
		},
		{
			label:  "2 docs, no WS",
			input:  "{}{}",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "2 docs, space separated",
			input:  "{} {}",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "2 docs, LF separated",
			input:  "{}\n{}",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "2 docs, CRLF separated",
			input:  "{}\r\n{}",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "3 docs, LF separated",
			input:  "{}\n{}\n{}",
			count:  3,
			errStr: io.EOF.Error(),
		},

		// Array of documents
		{
			label:  "array: no docs",
			input:  "[]",
			count:  0,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: one doc w/ WS",
			input:  "[ {} ]",
			count:  1,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 2 docs, no WS",
			input:  "[{},{}]",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 2 docs, space separated",
			input:  "[{}, {}]",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 2 docs, LF separated",
			input:  "[{},\n{}]",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 2 docs, CRLF separated",
			input:  "[{},\r\n{}]",
			count:  2,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 3 docs",
			input:  "[{},{},{}]",
			count:  3,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: 2 arrays",
			input:  "[{},{},{}]\n[{}]",
			count:  3,
			errStr: io.EOF.Error(),
		},
		{
			label:  "array: no comma",
			input:  "[{} {}]",
			count:  0,
			errStr: "expecting value-separator or end of array",
		},
		{
			label:  "array: not terminated",
			input:  "[{},{}",
			count:  1,
			errStr: "unexpected EOF",
		},

		// Non documents in stream
		{
			label:  "non-document",
			input:  `42`,
			count:  0,
			errStr: "Decode only supports object decoding",
		},
		{
			label:  "non-document after document",
			input:  `{} 42`,
			count:  1,
			errStr: "Decode only supports object decoding",
		},
		{
			label:  "non-document in array",
			input:  `[42]`,
			count:  0,
			errStr: "Decode only supports object decoding",
		},
		{
			label:  "start with array terminator",
			input:  `]{"a":"b"}`,
			count:  0,
			errStr: "Decode only supports object decoding",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			t.Parallel()
			var err error
			jsonReader := bufio.NewReader(bytes.NewReader([]byte(c.input)))
			jib, err := NewDecoder(jsonReader)
			if err != nil && err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}

			buf := make([]byte, 0, 256)
			var n int
			for err == nil {
				buf = buf[0:0]
				buf, err = jib.Decode(buf)
				if err != nil {
					break
				}
				n++
			}
			if n != c.count {
				t.Errorf("expected %d docs, but got %d", c.count, n)
			}
			if !strings.Contains(err.Error(), c.errStr) {
				t.Errorf("expected error with '%s', but got %v", c.errStr, err)
			}

		})
	}
}

// TestDepthLimit checks the ability to set a depth limit.
func TestDepthLimit(t *testing.T) {
	t.Parallel()

	testDepthLimit(t, `{"1":{"2":{"3":[{"5":"a"}]}}}`)
	testDepthLimit(t, `{"1":{"2":{"3":[["5","a"]]}}}`)
}

func testDepthLimit(t *testing.T, input string) {
	t.Helper()

	out := make([]byte, 0)

	jib, err := NewDecoder(bufio.NewReader(bytes.NewReader([]byte(input))))
	if err != nil {
		t.Fatal(err)
	}
	jib.MaxDepth(4)
	out, err = jib.Decode(out)
	if err == nil {
		t.Fatalf("expected error and got nil")
	}

	jib, err = NewDecoder(bufio.NewReader(bytes.NewReader([]byte(input))))
	if err != nil {
		t.Fatal(err)
	}
	jib.MaxDepth(5)
	_, err = jib.Decode(out)
	if err != nil {
		t.Fatalf("expected no error and got: %v", err)
	}

}
