// Copyright 2020 by David A. Golden. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package jibby_test

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

	// Do something with bson
	_ = bson
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

	// Do something with bson
	_ = bson
}
