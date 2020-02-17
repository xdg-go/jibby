# jibby - High-performance streaming JSON-to-BSON decoder in Go

[![GoDoc](https://godoc.org/github.com/xdg-go/jibby?status.svg)](https://godoc.org/github.com/xdg-go/jibby) [![Build Status](https://travis-ci.org/xdg-go/jibby.svg?branch=master)](https://travis-ci.org/xdg-go/jibby) [![codecov](https://codecov.io/gh/xdg-go/jibby/branch/master/graph/badge.svg)](https://codecov.io/gh/xdg-go/jibby) [![Go Report Card](https://goreportcard.com/badge/github.com/xdg-go/jibby)](https://goreportcard.com/report/github.com/xdg-go/jibby) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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

	jsonReader := bufio.NewReader(bytes.NewReader([]byte(json)))
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

Extended JSON support is incomplete.

# Limitations

* Maximum depth defaults to 200 levels of nesting (but is configurable)
* Only well-formed UTF-8 encoding (including optional BOM) is supported.
* Numbers (floats and ints) must conform to formats/limits of Go's
  [strconv](https://golang.org/pkg/strconv/) library.

# Testing

Jibby is extensively tested.

Jibby's JSON-to-BSON output is compared against reference output from the
[MongoDB Go driver](https://pkg.go.dev/go.mongodb.org/mongo-driver).

JSON parsing support is tested against data sets from Nicholas Seriot's
[Parsing JSON is a Minefield](http://seriot.ch/parsing_json.php) article.  It
behaves correctly against all "y" (must support) tests and "n" (must error) tests.
It passes all "i" (implementation defined) tests except for cases exceeding
Go's numerical precision or with invalid/unsupported Unicode encoding.

# Performance

TBD

# Copyright and License

Copyright 2020 by David A. Golden. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
