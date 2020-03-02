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
	"encoding/json"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

// TestExtJSON tests a targeted subset of the MongoDB BSON corpus tests with a
// single, "normal" (non-edge case) conversion for each type.
func TestExtJSON(t *testing.T) {
	cases := []unmarshalTestCase{
		{
			label:  "$oid",
			input:  `{"a" : {"$oid" : "56e1fc72e0c917e9c4714161"}}`,
			output: "1400000007610056E1FC72E0C917E9C471416100",
		},
		{
			label:  "$symbol",
			input:  `{"a": {"$symbol": ""}}`,
			output: "0D0000000E6100010000000000",
		},
		{
			label:  "$numberInt",
			input:  `{"i" : {"$numberInt": "0"}}`,
			output: "0C0000001069000000000000",
		},
		{
			label:  "$numberLong",
			input:  `{"a" : {"$numberLong" : "-9223372036854775808"}}`,
			output: "10000000126100000000000000008000",
		},
		{
			label:  "$numberDouble",
			output: "1000000001640081E97DF41022B14300",
			input:  `{"d" : {"$numberDouble": "1.23456789012345677E+18"}}`,
		},
		{
			label:  "$numberDouble NaN",
			output: "10000000016400000000000000F87F00",
			input:  `{"d": {"$numberDouble": "NaN"}}`,
		},
		{
			label:  "$numberDouble Inf",
			output: "10000000016400000000000000F07F00",
			input:  `{"d": {"$numberDouble": "Infinity"}}`,
		},
		{
			label:  "$numberDouble -Inf",
			output: "10000000016400000000000000F0FF00",
			input:  `{"d": {"$numberDouble": "-Infinity"}}`,
		},
		{
			label:  "$numberDecimal",
			input:  `{"d" : {"$numberDecimal" : "0.1000000000000000000000000000000000"}}`,
			output: "18000000136400000000000A5BC138938D44C64D31FC2F00",
		},
		{
			label:  "$binary",
			input:  `{"x" : { "$binary" : {"base64" : "c//SZESzTGmQ6OfR38A11A==", "subType" : "03"}}}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary, subtype too long",
			input:  `{"x" : { "$binary" : {"base64" : "c//SZESzTGmQ6OfR38A11A==", "subType" : "123"}}}`,
			errStr: "exceeds expected length 2",
		},
		{
			label:  "$binary, single type digit",
			input:  `{"x" : { "$binary" : {"base64" : "c//SZESzTGmQ6OfR38A11A==", "subType" : "3"}}}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary, keys reversed",
			input:  `{"x" : { "$binary" : {"subType" : "03", "base64" : "c//SZESzTGmQ6OfR38A11A=="}}}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary legacy",
			input:  `{"x" : { "$binary" : "c//SZESzTGmQ6OfR38A11A==", "$type" : "03"}}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary legacy, single type digit",
			input:  `{"x" : { "$binary" : "c//SZESzTGmQ6OfR38A11A==", "$type" : "3"}}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary legacy, keys reversed",
			input:  `{"x" : { "$type" : "03", "$binary" : "c//SZESzTGmQ6OfR38A11A==" }}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$binary legacy, keys reversed, single type digit",
			input:  `{"x" : { "$type" : "03", "$binary" : "c//SZESzTGmQ6OfR38A11A==" }}`,
			output: "1D000000057800100000000373FFD26444B34C6990E8E7D1DFC035D400",
		},
		{
			label:  "$code",
			input:  `{"a" : {"$code" : "abababababab"}}`,
			output: "190000000D61000D0000006162616261626162616261620000",
		},
		{
			label:  "$code $scope",
			input:  `{"a" : {"$code" : "\u00e9\u0000d", "$scope" : {}}}`,
			output: "1A0000000F61001200000005000000C3A9006400050000000000",
		},
		{
			label:  "$code $scope, keys reversed",
			input:  `{"a" : {"$scope" : {}, "$code" : "\u00e9\u0000d"}}`,
			output: "1A0000000F61001200000005000000C3A9006400050000000000",
		},
		{
			label:  "$timestamp",
			input:  `{"a" : {"$timestamp" : {"t" : 123456789, "i" : 42} } }`,
			output: "100000001161002A00000015CD5B0700",
		},
		{
			label:  "$timestamp, keys reversed",
			input:  `{"a" : {"$timestamp" : {"i" : 42, "t" : 123456789} } }`,
			output: "100000001161002A00000015CD5B0700",
		},
		{
			label:  "$regularExpression",
			input:  `{"a" : {"$regularExpression" : { "pattern": "abc", "options" : "im"}}}`,
			output: "0F0000000B610061626300696D0000",
		},
		{
			label:  "$regularExpression, keys reversed",
			input:  `{"a" : {"$regularExpression" : {"options" : "im", "pattern": "abc"}}}`,
			output: "0F0000000B610061626300696D0000",
		},
		{
			label:  "$regex string",
			input:  `{"a" : {"$regex" : "abc", "$options" : "im"}}`,
			output: "0F0000000B610061626300696D0000",
		},
		{
			label:  "$regex string, keys reversed",
			input:  `{"a" : {"$options" : "im", "$regex" : "abc"}}`,
			output: "0F0000000B610061626300696D0000",
		},
		{
			label:  "$regex document",
			input:  `{"a" : { "$regex": {"$regularExpression" : { "pattern": "abc", "options" : "im"}}, "$options" : "s"}}`,
			output: "2c000000036100240000000b2472656765780061626300696d0002246f7074696f6e73000200000073000000",
		},
		{
			label:  "$regex document, keys reversed",
			input:  `{"a" : { "$options" : "s", "$regex": {"$regularExpression" : { "pattern": "abc", "options" : "im"}}}}`,
			output: "2c0000000361002400000002246f7074696f6e73000200000073000b2472656765780061626300696d000000",
		},
		{
			label:  "$dbPointer",
			output: "1A0000000C610002000000620056E1FC72E0C917E9C471416100",
			input:  `{"a": {"$dbPointer": {"$ref": "b", "$id": {"$oid": "56e1fc72e0c917e9c4714161"}}}}`,
		},
		{
			label:  "$dbPointer, keys reversed",
			output: "1A0000000C610002000000620056E1FC72E0C917E9C471416100",
			input:  `{"a": {"$dbPointer": {"$id": {"$oid": "56e1fc72e0c917e9c4714161"}, "$ref": "b"}}}`,
		},
		{
			label:  "$date, numberLong",
			output: "10000000096100000000000000000000",
			input:  `{"a" : {"$date" : {"$numberLong" : "0"}}}`,
		},
		{
			label:  "$date, ISO 8601",
			output: "10000000096100000000000000000000",
			input:  `{"a" : {"$date" : "1970-01-01T00:00:00Z"}}`,
		},
		{
			label:  "$date, invalid object",
			input:  `{"a" : {"$date" : {"00000" : "0"}}}`,
			errStr: `expected "$numberLong"`,
		},
		{
			label:  "$maxKey",
			input:  `{"a" : {"$maxKey" : 1}}`,
			output: "080000007F610000",
		},
		{
			label:  "$minKey",
			input:  `{"a" : {"$minKey" : 1}}`,
			output: "08000000FF610000",
		},
		{
			label:  "$undefined",
			input:  `{"a" : {"$undefined" : true}}`,
			output: "0800000006610000",
		},
		{
			label:  "top level not extended JSON",
			input:  `{"$symbol": "abc"}`,
			output: "16000000022473796d626f6c00040000006162630000",
		},
	}

	testWithUnmarshal(t, cases, true)
}

// Rest of this file tests using the MongoDB BSON Corpus:
// https://github.com/mongodb/specifications/tree/master/source/bson-corpus
// Licensed under Creative Commons by-nc-sa

const dataDir = "testdata/mongodb-corpus/tests"

type validCorpusCase struct {
	Description       string `json:"description"`
	CanonicalBSON     string `json:"canonical_bson"`
	CanonicalExtJSON  string `json:"canonical_extjson"`
	RelaxedExtJSON    string `json:"relaxed_extjson"`
	DegenerateBSON    string `json:"degenerate_bson"`
	DegenerateExtJSON string `json:"degenerate_extjson"`
	Lossy             bool   `json:"lossy"`
}

type parseErrorCorpusCase struct {
	Description string `json:"description"`
	Input       string `json:"string"`
}

type corpusFile struct {
	Description string                 `json:"description"`
	TestKey     string                 `json:"test_key"`
	Valid       []validCorpusCase      `json:"valid"`
	ParseErrors []parseErrorCorpusCase `json:"parseErrors"`
}

func TestBSONCorpus(t *testing.T) {
	t.Parallel()

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		data, err := ioutil.ReadFile(filepath.Join(dataDir, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var testCase corpusFile
		err = json.Unmarshal(data, &testCase)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(f.Name(), func(t *testing.T) {
			if testCase.Valid != nil {
				testValidCorpusCases(t, testCase.Valid)
			}
			if testCase.ParseErrors != nil {
				testParseErrorCorpusCases(t, testCase.ParseErrors)
			}
		})
	}
}

func testValidCorpusCases(t *testing.T, cases []validCorpusCase) {
	t.Run("valid", func(t *testing.T) {
		for _, c := range cases {
			t.Run(c.Description, func(t *testing.T) {
				t.Parallel()

				if c.CanonicalExtJSON != "" && !c.Lossy {
					compareCorpusUnmarshal(t, c.CanonicalExtJSON, c.CanonicalBSON)
				}
				if c.DegenerateExtJSON != "" && !c.Lossy {
					compareCorpusUnmarshal(t, c.DegenerateExtJSON, c.CanonicalBSON)
				}
				if c.RelaxedExtJSON != "" {
					fromJibby, err := convertWithJibby([]byte(c.RelaxedExtJSON))
					if err != nil {
						t.Fatalf("jibby decoding: %v", err)
					}
					fromMongoDriver, err := convertWithGoDriver([]byte(c.RelaxedExtJSON))
					if err != nil {
						t.Fatalf("MongoDB driver decoding: %v", err)
					}
					if !bytes.Equal(fromJibby, fromMongoDriver) {
						t.Fatalf("Unmarshal doesn't match expected:\nGot:    %v\nExpect: %v", hex.EncodeToString(fromJibby), hex.EncodeToString(fromMongoDriver))
					}
				}
			})
		}
	})
}

func compareCorpusUnmarshal(t *testing.T, input string, output string) {
	expect, err := hex.DecodeString(output)
	if err != nil {
		t.Fatalf("error decoding test output: %v", err)
	}
	out := make([]byte, 0, 256)
	jsonReader := bufio.NewReader(bytes.NewReader([]byte(input)))
	jib, err := NewDecoder(jsonReader)
	jib.ExtJSON(true)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err = jib.Decode(out)
	if err != nil {
		t.Errorf("Decoding: %v", err)
	}

	// We expect only one document: check for EOF
	buf := make([]byte, 0)
	_, err = jib.Decode(buf)
	if err != io.EOF {
		t.Errorf("Expected io.EOF but got: %v", err)
	}

	if !bytes.Equal(out, expect) {
		t.Fatalf("Unmarshal doesn't match expected:\nGot:    %v\nExpect: %v", hex.EncodeToString(out), output)
	}
}

func testParseErrorCorpusCases(t *testing.T, cases []parseErrorCorpusCase) {
	t.Run("parse errors", func(t *testing.T) {
		for _, c := range cases {
			t.Run(c.Description, func(t *testing.T) {
				t.Parallel()
				_, err := convertWithJibby([]byte(c.Input))
				if err == nil {
					t.Fatalf("Expected error but got nil")
				}
				if !strings.Contains(err.Error(), "parse error") {
					t.Fatalf("Error didn't contain 'parse error': %v", err)
				}
			})
		}
	})
}
