package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/xdg-go/jibby"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: jibbyperf <json file>")
	}
	inputFile := os.Args[1]
	jsonData, err := ioutil.ReadFile(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	benchJibby(jsonData)
	benchMongoDriverRW(jsonData)
	benchNaive(jsonData)
}

func benchJibby(input []byte) {
	bson := make([]byte, 0, 256)

	jsonReader := bufio.NewReader(bytes.NewReader(input))
	jib, err := jibby.NewDecoder(jsonReader)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for {
		bson = bson[0:0]
		bson, err = jib.Decode(bson)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	reportResult("jibby", len(input), elapsed)
}

func benchMongoDriverRW(input []byte) {
	var err error
	jsonReader := (bytes.NewReader(input))

	vr, err := bsonrw.NewExtJSONValueReader(jsonReader, false)
	if err != nil {
		log.Fatal(err)
	}

	// first, we need to discover what mode we are in.
	// 1. doc mode, where each document is separated by 0 or more whitespace
	// 2. array mode, where each document is an entry in a top-level array
	var ar bsonrw.ArrayReader
	switch vr.Type() {
	case bsontype.EmbeddedDocument:
	case bsontype.Array:
		ar, err = vr.ReadArray()
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("JSON format unsupported by Go driver")
	}

	copier := bsonrw.NewCopier()
	start := time.Now()
	for {
		if ar != nil {
			evr, err := ar.ReadValue()
			if err != nil {
				if err == bsonrw.ErrEOA {
					break
				}
				log.Fatal(err)
			}

			if evr.Type() != bsontype.EmbeddedDocument {
				log.Fatal("JSON format unsupported by Go driver")
			}

			doc, err := copier.CopyDocumentToBytes(evr)
			if err != nil {
				log.Fatal(err)
			}
			_ = doc
		} else {
			doc, err := copier.CopyDocumentToBytes(vr)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			_ = doc
		}
	}
	elapsed := time.Since(start)
	reportResult("driver bsonrw", len(input), elapsed)
}

func benchNaive(input []byte) {
	jsonReader := (bytes.NewReader(input))
	dec := json.NewDecoder(jsonReader)

	start := time.Now()
	for dec.More() {
		var m map[string]interface{}
		err := dec.Decode(&m)
		if err != nil {
			log.Fatal(err)
		}
		buf, err := bson.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}
		_ = buf
	}
	elapsed := time.Since(start)
	reportResult("naive json->bson", len(input), elapsed)
}

func reportResult(label string, size int, elapsed time.Duration) {
	throughput := float64(size) / float64(elapsed.Microseconds())
	fmt.Printf("%15s %.2f MB/s\n", label, throughput)
}
