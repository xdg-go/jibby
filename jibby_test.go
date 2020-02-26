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

func TestJSONTestSuite_Passing(t *testing.T) {
	t.Helper()
	t.Parallel()
	files := getTestFiles(t, JSONTestSuite, "y", ".json")
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, filepath.Join(JSONTestSuite, f), nil)
		})
	}
}

func TestJibbyTestSuite_Passing(t *testing.T) {
	t.Helper()
	t.Parallel()
	files := getTestFiles(t, JibbyTestSuite, "y", ".json")
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, filepath.Join(JibbyTestSuite, f), nil)
		})
	}
}

// Some errors are allowed for implementation-defined behaviors.
var allowedErrors = []string{
	"detected unsupported", // 16/32-bit BOM
	"value out of range",   // large ints/floats
}

// We assume valid UTF-8; these files test handling invalid strings.
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

func TestJSONTestSuite_ImplDefined(t *testing.T) {
	t.Helper()
	t.Parallel()

	files := getTestFiles(t, JSONTestSuite, "i", ".json")
	for _, f := range files {
		if unsupportedTests[f] {
			continue
		}
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			testPassingConversion(t, filepath.Join(JSONTestSuite, f), allowedErrors)
		})
	}
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
		t.Fatalf("jibby doesn't match Go driver:\njibby:  %v\nDriver: %v", hex.EncodeToString(jibbyGot), hex.EncodeToString(driverGot))
	}
}

func TestJSONTestSuite_Failing(t *testing.T) {
	t.Parallel()

	files := getTestFiles(t, JSONTestSuite, "n", ".json")
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			t.Parallel()
			var err error
			var jibbyGot []byte
			text, err := ioutil.ReadFile(filepath.Join(JSONTestSuite, f))
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
		})
	}
}

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

func TestDepthLimit(t *testing.T) {
	t.Parallel()

	input := `{"1":{"2":{"3":[{"5":"a"}]}}}`
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
