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
