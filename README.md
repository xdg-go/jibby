# jibby - High-performance streaming JSON-to-BSON decoder in Go

[![Go Reference](https://pkg.go.dev/badge/github.com/xdg-go/jibby.svg)](https://pkg.go.dev/github.com/xdg-go/jibby)
[![Go Report Card](https://goreportcard.com/badge/github.com/xdg-go/jibby)](https://goreportcard.com/report/github.com/xdg-go/jibby)
[![Github Actions](https://github.com/xdg-go/jibby/actions/workflows/test.yml/badge.svg)](https://github.com/xdg-go/jibby/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/xdg-go/jibby/branch/master/graph/badge.svg)](https://codecov.io/gh/xdg-go/jibby)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> _jibby: A general term to describe an exceptionally positive vibe, attitude,
> or influence._
>
> ~ Urban Dictionary

The jibby package provide high-performance conversion of
[JSON](https://www.json.org/) objects to [BSON](http://bsonspec.org/)
documents.  Key features include:

* stream decoding - white space delimited or from a JSON array container
* no reflection
* minimal abstraction
* minimal copy
* allocation-friendly

# Examples

```
import (
	"bufio"
	"bytes"
	"log"

	"github.com/xdg-go/jibby"
)

func ExampleUnmarshal() {
	json := `{"a": 1, "b": "foo"}`
	bson := make([]byte, 0, 256)

	bson, err := jibby.Unmarshal([]byte(json), bson)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDecoder_Decode() {
	json := `{"a": 1, "b": "foo"}`
	bson := make([]byte, 0, 256)

	jsonReader := bufio.NewReaderSize(bytes.NewReader([]byte(json)), 8192)
	jib, err := jibby.NewDecoder(jsonReader)
	if err != nil {
		log.Fatal(err)
	}

	bson, err = jib.Decode(bson)
	if err != nil {
		log.Fatal(err)
	}
}
```

# Extended JSON

Jibby optionally supports the [MongoDB Extended JSON
v2](https://docs.mongodb.com/manual/reference/mongodb-extended-json/index.html)
format.  There is limited support for the v1 format -- specifically, the
`$type` and `$regex` keys use heuristics to determine whether these are
extended JSON or MongoDB query operators.

Escape sequences are not supported in Extended JSON keys or number formats,
only in naturally textual fields like `$symbol`, `$code`, etc.  In practice,
MongoDB Extended JSON generators should never output escape sequences in keys
and number fields anyway.

# Limitations

* Maximum depth defaults to 200 levels of nesting (but is configurable)
* Only well-formed UTF-8 encoding (including optional BOM) is supported.
* Numbers (floats and ints) must conform to formats/limits of Go's
  [strconv](https://golang.org/pkg/strconv/) library.
* Escape sequences not supported in extended JSON keys and some extended JSON
  values.

# Testing

Jibby is extensively tested.

Jibby's JSON-to-BSON output is compared against reference output from the
[MongoDB Go driver](https://pkg.go.dev/go.mongodb.org/mongo-driver).  Extended
JSON conversion is tested against the [MongoDB BSON
Corpus](https://github.com/mongodb/specifications/tree/master/source/bson-corpus).

JSON parsing support is tested against data sets from Nicholas Seriot's
[Parsing JSON is a Minefield](http://seriot.ch/parsing_json.php) article.  It
behaves correctly against all "y" (must support) tests and "n" (must error) tests.
It passes all "i" (implementation defined) tests except for cases exceeding
Go's numerical precision or with invalid/unsupported Unicode encoding.

# Performance

Performance varies based on the shape of the input data.

For a 92 MB mixed JSON dataset with some extended JSON:
```
           jibby 283.46 MB/s
   jibby extjson 207.42 MB/s
   driver bsonrw 43.77 MB/s
naive json->bson 43.25 MB/s
```

For a 4.3 MB pure JSON dataset with lots of arrays:
```
           jibby 107.15 MB/s
   jibby extjson 123.76 MB/s
   driver bsonrw 25.68 MB/s
naive json->bson 32.78 MB/s
```

The `jibby` and `jibby extjson` figures are jibby without and with extended
JSON enabled, respectively.  The `driver bsonrw` figures use the MongoDB Go
driver in a streaming mode with `bsonrw.NewExtJSONValueReader`.  The `naive
json->bson` figures use Go's `encoding/json` to decode to
`map[string]interface{}` and the Go driver's `bson.Marshal` function.

# Copyright and License

Copyright 2020 by David A. Golden. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
