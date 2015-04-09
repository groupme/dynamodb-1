package main

import "time"

type Model struct {
	Bool bool
	// BoolSlice   []bool
	Byte        []byte
	ByteSlice   [][]byte
	Int         int
	IntSlice    []int
	String      string
	StringSlice []string
	Time        time.Time
}
