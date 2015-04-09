// DO NOT EDIT.
// Auto-generated template file by dynamodb-marshal.

package main

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"time"
	"unicode/utf8"
)

func (m *Model) Encode(buf *bytes.Buffer) {
	buf.WriteString(`{"Bool":{"N":"`)
	if m.Bool {
		buf.WriteByte('1')
	} else {
		buf.WriteByte('0')
	}
	buf.WriteString(`"},"Byte":{"B":"`)
	buf.WriteString(base64.StdEncoding.EncodeToString(m.Byte))
	buf.WriteString(`"},"ByteSlice":{"BS":[`)
	for idx, elem := range m.ByteSlice {
		buf.WriteByte('"')
		buf.WriteString(base64.StdEncoding.EncodeToString(elem))
		if idx == len(m.ByteSlice)-1 {
			buf.WriteByte('"')
		} else {
			buf.WriteString(`",`)
		}
	}
	buf.WriteString(`]},"Int":{"N":"`)
	buf.WriteString(strconv.FormatInt(int64(m.Int), 10))
	buf.WriteString(`"},"IntSlice":{"NS":[`)
	for idx, elem := range m.IntSlice {
		buf.WriteByte('"')
		buf.WriteString(strconv.FormatInt(int64(elem), 10))
		if idx == len(m.IntSlice)-1 {
			buf.WriteByte('"')
		} else {
			buf.WriteString(`",`)
		}
	}
	buf.WriteString(`]},"String":{"S":"`)
	toJSON(m.String, buf)
	buf.WriteString(`"},"StringSlice":{"SS":[`)
	for idx, elem := range m.StringSlice {
		buf.WriteByte('"')
		toJSON(elem, buf)
		if idx == len(m.StringSlice)-1 {
			buf.WriteByte('"')
		} else {
			buf.WriteString(`",`)
		}
	}
	buf.WriteString(`]},"Time":{"N":"`)
	buf.WriteString(strconv.FormatInt(m.Time.UnixNano(), 10))
	buf.WriteString(`"}}`)
}

func (m *Model) Decode(data map[string]map[string]interface{}) {
	if val, ok := data["Bool"]["N"].(string); ok {
		if val == "1" {
			m.Bool = true
		} else if val == "0" {
			m.Bool = false
		}
	}
	if val, ok := data["Byte"]["B"].(string); ok {
		m.Byte, _ = base64.StdEncoding.DecodeString(val)
	}
	if vals, ok := data["ByteSlice"]["BS"].([]interface{}); ok {
		for _, sval := range vals {
			val := sval.(string)
			tmp, _ := base64.StdEncoding.DecodeString(val)
			m.ByteSlice = append(m.ByteSlice, tmp)
		}
	}
	if val, ok := data["Int"]["N"].(string); ok {
		tmp, _ := strconv.ParseInt(val, 10, 64)
		m.Int = int(tmp)
	}
	if vals, ok := data["IntSlice"]["NS"].([]interface{}); ok {
		for _, sval := range vals {
			val := sval.(string)
			tmp, _ := strconv.ParseInt(val, 10, 64)
			m.IntSlice = append(m.IntSlice, int(tmp))
		}
	}
	if val, ok := data["String"]["S"].(string); ok {
		m.String = val
	}
	if vals, ok := data["StringSlice"]["SS"].([]interface{}); ok {
		for _, sval := range vals {
			val := sval.(string)
			m.StringSlice = append(m.StringSlice, val)
		}
	}
	if val, ok := data["Time"]["N"].(string); ok {
		tmp, _ := strconv.ParseInt(val, 10, 64)
		m.Time = time.Unix(0, tmp).UTC()
	}
}


// Adapted from the encoding/json package in the standard
// library.
const hex = "0123456789abcdef"

func toJSON(s string, buf *bytes.Buffer) {
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			if start < i {
				buf.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				buf.WriteByte('\\')
				buf.WriteByte(b)
			case '\n':
				buf.WriteByte('\\')
				buf.WriteByte('n')
			case '\r':
				buf.WriteByte('\\')
				buf.WriteByte('r')
			default:
				buf.WriteString("\\u00")
				buf.WriteByte(hex[b>>4])
				buf.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteString("\\ufffd")
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
}
