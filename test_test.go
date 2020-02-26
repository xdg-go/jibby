package jibby

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

type unmarshalTestCase struct {
	label  string
	input  string
	output string
	errStr string
}

func testWithUnmarshal(t *testing.T, cases []unmarshalTestCase, extJSON bool) {
	t.Helper()

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			t.Parallel()

			var err error
			buf := make([]byte, 0, 256)
			if extJSON {
				buf, err = UnmarshalExtJSON([]byte(c.input), buf)
			} else {
				buf, err = Unmarshal([]byte(c.input), buf)
			}
			if c.errStr != "" {
				var got string
				if err != nil {
					got = err.Error()
				}
				if !strings.Contains(got, c.errStr) {
					t.Errorf("expected error with '%s', but got %v", c.errStr, got)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				} else {
					c.output = strings.ToLower(c.output)
					expect, err := hex.DecodeString(c.output)
					if err != nil {
						t.Fatalf("error decoding test output: %v", err)
					}
					if !bytes.Equal(expect, buf) {
						t.Fatalf("Unmarshal doesn't match expected:\nGot:    %v\nExpect: %v", hex.EncodeToString(buf), c.output)
					}

				}
			}
		})
	}
}

func getTestFiles(t *testing.T, dir, prefix, suffix string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	keep := make([]string, 0)
	for _, file := range files {
		name := file.Name()
		if prefix != "" {
			if !strings.HasPrefix(name, prefix) {
				continue
			}
		}
		if suffix != "" {
			if !strings.HasSuffix(name, suffix) {
				continue
			}
		}
		keep = append(keep, name)
	}

	return keep
}

func convertWithJibby(input []byte) ([]byte, error) {
	jsonReader := bufio.NewReader(bytes.NewReader(input))
	jib, err := NewDecoder(jsonReader)
	jib.ExtJSON(true)
	if err != nil {
		return nil, err
	}
	return jib.Decode(make([]byte, 0, 256))
}

func convertWithGoDriver(input []byte) ([]byte, error) {
	var got bson.Raw
	err := bson.UnmarshalExtJSON(input, false, &got)
	return got, err
}

func objectify(input []byte) []byte {
	// Skip over BOM and leading spaces
	i := bomLength(input)
	for i < len(input) {
		if input[i] != ' ' {
			break
		}
		i++
	}
	if input[i] != '{' {
		object := make([]byte, 0)
		object = append(object, input[0:i]...)
		object = append(object, []byte(`{"a":`)...)
		object = append(object, input[i:]...)
		object = append(object, '}')
		return object
	}
	return input
}

func bomLength(input []byte) int {
	if len(input) < 2 {
		return 0
	}
	if bytes.Equal(input[0:2], utf16BEBOM) || bytes.Equal(input[0:2], utf16LEBOM) {
		return 2
	}
	if len(input) < 3 {
		return 0
	}
	if bytes.Equal(input[0:3], utf8BOM) {
		return 3
	}
	if len(input) < 4 {
		return 0
	}
	if bytes.Equal(input[0:4], utf32BEBOM) || bytes.Equal(input[0:4], utf32LEBOM) {
		return 4
	}
	return 0
}
