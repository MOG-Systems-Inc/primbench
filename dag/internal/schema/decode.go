package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Decode parses one JSON document into the values the validator walks:
// map[string]any, []any, string, json.Number, bool and nil, the same values
// jsonschema.UnmarshalJSON returns. It reads the bytes once, with no
// reflection, and allocates only the values themselves: one map or slice per
// object or array, one string per key and string value, one per number. A
// string with an escape or bytes that are not UTF-8 is handed to
// encoding/json, so every string decodes exactly as encoding/json decodes it.
func Decode(raw []byte) (any, error) {
	d := decoder{b: raw}
	d.space()
	v, err := d.value()
	if err != nil {
		return nil, err
	}
	d.space()
	if d.i != len(d.b) {
		return nil, errors.New("invalid character after top-level value")
	}
	return v, nil
}

type decoder struct {
	b []byte
	i int
}

func (d *decoder) errf(format string, a ...any) error {
	return fmt.Errorf("json: offset %d: "+format, append([]any{d.i}, a...)...)
}

func (d *decoder) space() {
	for d.i < len(d.b) {
		switch d.b[d.i] {
		case ' ', '\t', '\n', '\r':
			d.i++
		default:
			return
		}
	}
}

func (d *decoder) value() (any, error) {
	if d.i >= len(d.b) {
		return nil, d.errf("unexpected end of input")
	}
	switch c := d.b[d.i]; {
	case c == '{':
		return d.object()
	case c == '[':
		return d.array()
	case c == '"':
		return d.str()
	case c == '-' || (c >= '0' && c <= '9'):
		return d.number()
	case d.lit("true"):
		return true, nil
	case d.lit("false"):
		return false, nil
	case d.lit("null"):
		return nil, nil
	default:
		return nil, d.errf("invalid character %q looking for beginning of value", c)
	}
}

func (d *decoder) lit(s string) bool {
	if len(d.b)-d.i >= len(s) && string(d.b[d.i:d.i+len(s)]) == s {
		d.i += len(s)
		return true
	}
	return false
}

func (d *decoder) object() (any, error) {
	d.i++ // {
	m := make(map[string]any, 8)
	d.space()
	if d.i < len(d.b) && d.b[d.i] == '}' {
		d.i++
		return m, nil
	}
	for {
		d.space()
		if d.i >= len(d.b) || d.b[d.i] != '"' {
			return nil, d.errf("expected an object key")
		}
		k, err := d.str()
		if err != nil {
			return nil, err
		}
		d.space()
		if d.i >= len(d.b) || d.b[d.i] != ':' {
			return nil, d.errf("expected ':' after object key")
		}
		d.i++
		d.space()
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		m[k] = v
		d.space()
		if d.i >= len(d.b) {
			return nil, d.errf("unexpected end of input in object")
		}
		switch d.b[d.i] {
		case ',':
			d.i++
		case '}':
			d.i++
			return m, nil
		default:
			return nil, d.errf("expected ',' or '}' in object")
		}
	}
}

func (d *decoder) array() (any, error) {
	d.i++ // [
	a := make([]any, 0, 4)
	d.space()
	if d.i < len(d.b) && d.b[d.i] == ']' {
		d.i++
		return a, nil
	}
	for {
		d.space()
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		a = append(a, v)
		d.space()
		if d.i >= len(d.b) {
			return nil, d.errf("unexpected end of input in array")
		}
		switch d.b[d.i] {
		case ',':
			d.i++
		case ']':
			d.i++
			return a, nil
		default:
			return nil, d.errf("expected ',' or ']' in array")
		}
	}
}

// str reads a string. The plain case, no escape and valid UTF-8, is one
// allocation; anything else is encoding/json's.
func (d *decoder) str() (string, error) {
	start := d.i
	d.i++ // "
	plain := true
	for d.i < len(d.b) {
		c := d.b[d.i]
		switch {
		case c == '"':
			d.i++
			if plain && utf8.Valid(d.b[start+1:d.i-1]) {
				return string(d.b[start+1 : d.i-1]), nil
			}
			var s string
			if err := json.Unmarshal(d.b[start:d.i], &s); err != nil {
				return "", err
			}
			return s, nil
		case c == '\\':
			plain = false
			d.i += 2
		case c < 0x20:
			return "", d.errf("invalid control character in string")
		default:
			d.i++
		}
	}
	return "", d.errf("unexpected end of input in string")
}

// number reads a number by the JSON grammar and keeps its text, as
// json.Decoder.UseNumber does.
func (d *decoder) number() (any, error) {
	start := d.i
	if d.b[d.i] == '-' {
		d.i++
	}
	switch {
	case d.i < len(d.b) && d.b[d.i] == '0':
		d.i++
	case d.i < len(d.b) && d.b[d.i] >= '1' && d.b[d.i] <= '9':
		d.digits()
	default:
		return nil, d.errf("invalid number")
	}
	if d.i < len(d.b) && d.b[d.i] == '.' {
		d.i++
		if !d.digits() {
			return nil, d.errf("invalid number: no digit after '.'")
		}
	}
	if d.i < len(d.b) && (d.b[d.i] == 'e' || d.b[d.i] == 'E') {
		d.i++
		if d.i < len(d.b) && (d.b[d.i] == '+' || d.b[d.i] == '-') {
			d.i++
		}
		if !d.digits() {
			return nil, d.errf("invalid number: no digit in exponent")
		}
	}
	return json.Number(d.b[start:d.i]), nil
}

func (d *decoder) digits() bool {
	n := d.i
	for d.i < len(d.b) && d.b[d.i] >= '0' && d.b[d.i] <= '9' {
		d.i++
	}
	return d.i > n
}
