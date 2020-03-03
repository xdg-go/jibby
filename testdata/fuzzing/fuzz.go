// +build gofuzz

package fuzzing

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/xdg-go/jibby"
	"go.mongodb.org/mongo-driver/bson"
)

var ErrPanicked = errors.New("Panicked")
var ErrIgnore = errors.New("Ignore")

func FuzzJSON(data []byte) int {
	if shouldSkip(data, false) {
		return 0
	}

	score := 0

	jsonErr := unmarshalWithJson(data)
	if jsonErr == ErrIgnore || jsonErr == ErrPanicked {
		return 0
	}

	jibbyOut := make([]byte, 0)
	_, jibbyErr := jibby.Unmarshal(data, jibbyOut)

	if jibbyErr != nil && jsonErr == nil {
		fmt.Printf("input : %s\n", trim(string(data)))
		panic(fmt.Sprintf("jibby errors when json succeeds: %v", jibbyErr))
	}

	if jibbyErr == nil && jsonErr != nil {
		fmt.Printf("input : %s\n", trim(string(data)))
		panic(fmt.Sprintf("jibby succeeds when json errors: %v", jsonErr))
	}

	// Increase score if parse sucessful
	if jibbyErr == nil {
		score = 1
	}

	return score
}

func FuzzXJSON(data []byte) int {
	if shouldSkip(data, true) {
		return 0
	}

	driverOut, driverErr := unmarshalWithDriver(data)
	if driverErr == ErrPanicked {
		return 0
	}

	jibbyOut := make([]byte, 0)
	jibbyOut, jibbyErr := jibby.UnmarshalExtJSON(data, jibbyOut)

	if jibbyErr != nil && driverErr == nil {
		if isDriverFalseNegative(jibbyErr) {
			return 0
		}
		fmt.Printf("input : %s\n", trim(string(data)))
		panic(fmt.Sprintf("driver succeeds when jibby errors: %v", jibbyErr))
	}

	if jibbyErr == nil && driverErr != nil {
		fmt.Printf("input : %s\n", trim(string(data)))
		panic(fmt.Sprintf("jibby succeeds when driver errors: %v", driverErr))
	}

	// If parse failed, no need to compare further
	if jibbyErr != nil {
		return 0
	}

	if !bytes.Equal(jibbyOut, driverOut) {
		fmt.Printf("jibby : %s\n", hex.EncodeToString(jibbyOut))
		fmt.Printf("driver: %s\n", hex.EncodeToString(driverOut))
		panic("not equal")
	}

	return 1
}

func unmarshalWithJson(data []byte) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = ErrPanicked
		}
	}()

	var jsonOut map[string]interface{}
	jsonErr := json.Unmarshal(data, &jsonOut)
	if jsonErr != nil && strings.Contains(jsonErr.Error(), "after top-level value") {
		return ErrIgnore
	}

	return jsonErr
}

func unmarshalWithDriver(data []byte) (out []byte, err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = ErrPanicked
		}
	}()

	var driverOut bson.Raw
	driverErr := bson.UnmarshalExtJSON(data, false, &driverOut)
	return []byte(driverOut), driverErr
}

func trim(s string) string {
	if len(s) < 160 {
		return s
	}

	return s[0:160] + "..."
}

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}
var objectRE = regexp.MustCompile(`^\s*\{`)
var dollarCodeRE = regexp.MustCompile(`\{\s*"\$code"\s*:`)
var longSubtypeRE = regexp.MustCompile(`("\$type"\s*:\s*"...|"subType"\s*:\s*"...)`)

func shouldSkip(data []byte, extjson bool) bool {
	if len(data) > 2 && bytes.Equal(data[0:3], utf8BOM) {
		// encoding/json doens't support UTF-8 BOM
		return true
	}

	if !objectRE.Match(data) {
		// jibby only supports top level object.  Ignore array framing for fuzz
		// testing.
		return true
	}

	if extjson {
		if dollarCodeRE.Match(data) {
			// GODRIVER-1502: driver mishandles $code validation
			return true
		}
		if longSubtypeRE.Match(data) {
			// GODRIVER-1505: driver allows long binary subtypes
			return true
		}
	}

	return false
}

var driverFalseNegativeErrStrings = map[string]string{
	"$dbPointer.$id must be BSON type 7, not type 2": "GODRIVER-1501",
	"control characters not allowed in strings":      "GODRIVER-1503",
	"but attempted to read embedded document":        "GODRIVER-1504",
}

// isDriverFalseNegative returns true for errors detected by jibby that the
// driver should have detected but failed to do so.
func isDriverFalseNegative(jibbyErr error) bool {
	_, ok := driverFalseNegativeErrStrings[jibbyErr.Error()]
	return ok
}
