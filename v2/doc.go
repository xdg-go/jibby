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
// Extended JSON
//
// Jibby optionally supports the MongoDB Extended JSON v2 format
// (https://docs.mongodb.com/manual/reference/mongodb-extended-json/index.html).
// There is limited support for the v1 format -- specifically, the `$type` and
// `$regex` keys use heuristics to determine whether these are extended JSON or
// MongoDB query operators.
//
// Escape sequences are not supported in Extended JSON keys or number formats,
// only in naturally textual fields like `$symbol`, `$code`, etc.  In practice,
// MongoDB Extended JSON generators should never output escape sequences in keys
// and number fields anyway.
//
// Testing
//
// Jibby is extensively tested.
//
// Jibby's JSON-to-BSON output is compared against reference output from the
// MongoDB Go driver.  Extended JSON conversion is tested against the MongoDB
// BSON Corpus:
// https://github.com/mongodb/specifications/tree/master/source/bson-corpus.
//
// JSON parsing support is tested against data sets from Nicholas Seriot's
// Parsing JSON is a Minefield article (http://seriot.ch/parsing_json.php).  It
// behaves correctly against all "y" (must support) tests and "n" (must error)
// tests.  It passes all "i" (implementation defined) tests except for cases
// exceeding Go's numerical precision or with invalid/unsupported Unicode
// encoding.
package jibby
