package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var testModel = &Model{
	Bool: false,
	// BoolSlice:   []bool{true, false},
	Byte:        []byte{'{', '}'},
	ByteSlice:   [][]byte{[]byte{'{', '}'}},
	Int:         1234567890,
	IntSlice:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	String:      "hello world",
	StringSlice: []string{"hello", "world"},
	Time:        time.Now(),
}

func BenchmarkEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		testModel.Encode(&buf)
		if buf.Len() == 0 {
			b.Error("zero buffer")
		}
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf, err := json.Marshal(testModel)
		if err != nil {
			b.Error(err)
		}
		if len(buf) == 0 {
			b.Error("zero buffer")
		}
	}
}
