// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby

import "testing"

// TestUnmarshal tests both the Unmarshal function and various primitive types,
// including some error cases where needed for test coverage.
//
// Some tests adapted from the MongoDB BSON Corpus, licensed CC by-sa-nc:
// https://github.com/mongodb/specifications/blob/master/source/bson-corpus/bson-corpus.rst
func TestUnmarshal(t *testing.T) {
	t.Parallel()

	cases := []unmarshalTestCase{
		// Empty
		{
			label:  "empty doc",
			input:  `{}`,
			output: "0500000000",
		},
		{
			label:  "empty subdoc",
			input:  `{"":{}}`,
			output: "0c0000000300050000000000",
		},
		// True
		{
			label:  "true ok",
			input:  `{"b" : true}`,
			output: "090000000862000100",
		},
		{
			label:  "true not ok",
			input:  `{"b" : t, "c": 1}`,
			errStr: "expecting true",
		},
		// False
		{
			label:  "false ok",
			input:  `{"b" : false}`,
			output: "090000000862000000",
		},
		{
			label:  "false not ok",
			input:  `{"b" : fake}`,
			errStr: "expecting false",
		},
		// Null
		{
			label:  "null ok",
			input:  `{"a" : null}`,
			output: "080000000A610000",
		},
		{
			label:  "null not ok",
			input:  `{"a" : nul}`,
			errStr: "expecting null",
		},
		// String
		{
			label:  "Empty string",
			input:  `{"a" : ""}`,
			output: "0D000000026100010000000000",
		},
		{
			label:  "Single character",
			input:  `{"a" : "b"}`,
			output: "0E00000002610002000000620000",
		},
		{
			label:  "Multi-character",
			input:  `{"a" : "abababababab"}`,
			output: "190000000261000D0000006162616261626162616261620000",
		},
		{
			label:  "two-byte UTF-8 (\u00e9)",
			input:  `{"a" : "\u00e9\u00e9\u00e9\u00e9\u00e9\u00e9"}`,
			output: "190000000261000D000000C3A9C3A9C3A9C3A9C3A9C3A90000",
		},
		{
			label:  "three-byte UTF-8 (\u2606)",
			input:  `{"a" : "\u2606\u2606\u2606\u2606"}`,
			output: "190000000261000D000000E29886E29886E29886E298860000",
		},
		{
			label:  "Outside BMP with surrogates (\U0001D11E)",
			input:  `{"a" : "\uD834\uDD1E"}`,
			output: "1100000002610005000000f09d849e0000",
		},
		{
			label:  "Lone surrogate",
			input:  `{"a" : "\uD834"}`,
			output: "1000000002610004000000efbfbd0000",
		},
		{
			label:  "Lone surrogate with trailing text",
			input:  `{"a" : "\uD834a"}`,
			output: "1100000002610005000000efbfbd610000",
		},
		{
			label:  "Lone surrogate with trailing non-unicode escape",
			input:  `{"a" : "\uD834\n"}`,
			output: "1100000002610005000000efbfbd0a0000",
		},
		{
			label:  "Lone surrogate with trailing unicode escape",
			input:  `{"a" : "\uD834\u00e9"}`,
			output: "1200000002610006000000efbfbdc3a90000",
		},
		{
			label:  "Embedded nulls",
			input:  `{"a" : "ab\u0000bab\u0000babab"}`,
			output: "190000000261000D0000006162006261620062616261620000",
		},
		{
			label:  "Required escapes",
			input:  `{"a":"ab\\\"\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001fab"}`,
			output: "320000000261002600000061625C220102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F61620000",
		},
		{
			label:  "escape on string copy buffer boundary",
			input:  `{"a" : "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"}`,
			output: "4d000000026100410000006161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161610a0000",
		},
		{
			label:  "unicode surrogate escape on buffer boundary",
			input:  `{"a" : "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\uD834\uDD1E"}`,
			output: "4b0000000261003f00000061616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161f09d849e0000",
		},
		{
			label:  "unicode bad surrogate escape on buffer boundary",
			input:  `{"a" : "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\uD834\n"}`,
			output: "4a0000000261003e000000616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161efbfbd0a0000",
		},
		{
			label:  "invalid unicode escape",
			input:  `{"a" : "\u00zz"}`,
			errStr: "converting unicode escape",
		},
		{
			label:  "invalid unicode escape",
			input:  `{"a" : "\u+062"}`,
			errStr: "converting unicode escape",
		},
		{
			label:  "invalid unicode escape",
			input:  `{"a" : "\u-062"}`,
			errStr: "converting unicode escape",
		},
		{
			label:  "invalid unicode escape in second surrogate pair",
			input:  `{"a" : "\ud834\u-062"}`,
			errStr: "converting unicode escape",
		},
		{
			label:  "unknown escape",
			input:  `{"a" : "\U00e9"}`,
			errStr: "unknown escape",
		},
		{
			label:  "control character unescaped",
			input:  "{\"a\" : \"\x07\"}",
			errStr: "control characters",
		},
		// Int32
		{
			label:  "MinInt32",
			input:  `{"i" : -2147483648}`,
			output: "0C0000001069000000008000",
		},
		{
			label:  "MaxInt32",
			input:  `{"i" : 2147483647}`,
			output: "0C000000106900FFFFFF7F00",
		},
		{
			label:  "-1",
			input:  `{"i" : -1}`,
			output: "0C000000106900FFFFFFFF00",
		},
		{
			label:  "0",
			input:  `{"i" : 0}`,
			output: "0C0000001069000000000000",
		},
		{
			label:  "1",
			input:  `{"i" : 1}`,
			output: "0C0000001069000100000000",
		},
		{
			label:  "bad int",
			input:  `{"d" : 1234abc}`,
			errStr: "int conversion",
		},
		{
			label:  "bad int with underscore",
			input:  `{"d" : 123_456}`,
			errStr: "invalid character",
		},
		{
			label:  "bad int",
			input:  `{"d" : -+1234}`,
			errStr: "invalid character",
		},
		{
			label:  "leading zero",
			input:  `{"d" : 02}`,
			errStr: "leading zero",
		},
		{
			label:  "leading zero",
			input:  `{"d" : -02}`,
			errStr: "leading zero",
		},
		{
			label:  "missing number",
			input:  `{"d" : }`,
			errStr: "invalid character",
		},
		{
			label:  "missing number",
			input:  `{"d" : , "e": 1}`,
			errStr: "invalid character",
		},
		{
			label:  "leading plus",
			input:  `{"d" : +1}`,
			errStr: "invalid character",
		},
		{
			label:  "number missing",
			input:  `{"d" : -}`,
			errStr: "number not found",
		},
		// Int64
		{
			label:  "MinInt64",
			input:  `{"a" : -9223372036854775808}`,
			output: "10000000126100000000000000008000",
		},
		{
			label:  "MaxInt64",
			input:  `{"a" : 9223372036854775807}`,
			output: "10000000126100FFFFFFFFFFFFFF7F00",
		},
		// Float
		{
			label:  "+1.0",
			input:  `{"d" : 1.0}`,
			output: "10000000016400000000000000F03F00",
		},
		{
			label:  "-1.0",
			input:  `{"d" : -1.0}`,
			output: "10000000016400000000000000F0BF00",
		},
		{
			label:  "0.0",
			input:  `{"d" : 0.0}`,
			output: "10000000016400000000000000000000",
		},
		{
			label:  "0e0",
			input:  `{"d" : 0e0}`,
			output: "10000000016400000000000000000000",
		},
		{
			label:  "2000000000000000000",
			input:  `{"d" : 200000000000000000000 }`,
			output: "10000000016400408cb5781daf254400",
		},
		{
			label:  "bad float trailing decimal",
			input:  `{"d" : 1.}`,
			errStr: "decimal must be followed by digit",
		},
		{
			label:  "bad float decimal without number",
			input:  `{"d" : 1.e1}`,
			errStr: "decimal must be followed by digit",
		},
		{
			label:  "-.0",
			input:  `{"d":-.0}`,
			errStr: "invalid character",
		},
		{
			label:  "bad float",
			input:  `{"d" : -1.0a0}`,
			errStr: "float conversion",
		},
		{
			label:  "number too long to parse",
			input:  `{ "a": 0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001 }`,
			errStr: "number too long",
		},
		// Multi-key
		{
			label:  "multikey",
			input:  `{"a":true, "b":false}`,
			output: "0d000000086100010862000000",
		},
		{
			label:  "multi-array",
			input:  `{"a":["b","c"]}`,
			output: "1f000000046100170000000230000200000062000231000200000063000000",
		},
		// Truncation
		{
			label:  "truncated key",
			input:  `{"a`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated string",
			input:  `{"a":"hello`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated integer",
			input:  `{"a":123`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated float",
			input:  `{"a":123.45`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated true",
			input:  `{"b" : t`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated false",
			input:  `{"b" : f`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated null",
			input:  `{"a" : n`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated array",
			input:  `{"a" : [`,
			errStr: "unexpected EOF",
		},
		{
			label:  "truncated object",
			input:  `{`,
			errStr: "unexpected EOF",
		},
		// structural errors
		{
			label:  "first value key not string",
			input:  `{ 123:456 }`,
			errStr: "expecting key or end of object",
		},
		{
			label:  "second value key not string",
			input:  `{ "a": 457, 123:456 }`,
			errStr: "expecting opening quote of key",
		},
		{
			label:  "first value missing colon",
			input:  `{ "a" 457 }`,
			errStr: "expecting ':'",
		},
		{
			label:  "second value missing colon",
			input:  `{ "a": 457, "b" 789 }`,
			errStr: "expecting ':'",
		},
		{
			label:  "third value not delimited",
			input:  `{ "a": 457, "b": 789 "c":123 }`,
			errStr: "expecting value-separator or end of object",
		},
		{
			label:  "third array value not delimited",
			input:  `{ "a": [ "hello", "world" 123 ] }`,
			errStr: "expecting value-separator or end of array",
		},
		{
			label:  "first array value invalid",
			input:  `{ "a": [ 123abc, "hello"] }`,
			errStr: "parse error",
		},
		{
			label:  "second array value invalid",
			input:  `{ "a": [ "hello", 123abc ] }`,
			errStr: "parse error",
		},
	}

	testWithUnmarshal(t, cases, false)
}
