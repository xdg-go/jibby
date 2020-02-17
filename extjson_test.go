package jibby

import "testing"

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
	}

	testWithUnmarshal(t, cases, true)
}
