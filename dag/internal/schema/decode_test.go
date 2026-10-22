package schema

import (
	"bytes"
	"io/fs"
	"reflect"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Decode returns what jsonschema.UnmarshalJSON returns, and refuses what it
// refuses.
func TestDecodeMatchesUnmarshalJSON(t *testing.T) {
	docs := []string{
		`{}`, `[]`, `null`, `true`, `false`, `0`, `-0`, `12`, `-1.5e+10`, `3E-2`, `""`,
		`{"a":1,"b":[1,2,{"c":null}],"d":"x","e":true,"f":false}`,
		" \n\t{ \"a\" : [ 1 , 2 ] }\r\n",
		`{"esc":"a\"b\\c\/dé😀\n"}`,
		`{"utf8":"Ünïcödé ✓"}`,
		`{"dup":1,"dup":2}`,
		`{"n":12345678901234567890123}`,
		"{\"bad\":\"\xff\"}",
	}
	sub, err := fs.Sub(embedded, "schemas")
	if err != nil {
		t.Fatal(err)
	}
	files, _ := fs.Glob(sub, "*.json")
	for _, f := range files {
		b, _ := fs.ReadFile(sub, f)
		docs = append(docs, string(b))
	}
	for _, doc := range docs {
		want, werr := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(doc)))
		got, gerr := Decode([]byte(doc))
		if (werr != nil) != (gerr != nil) {
			t.Errorf("%.60q: UnmarshalJSON error %v, Decode error %v", doc, werr, gerr)
			continue
		}
		if werr == nil && !reflect.DeepEqual(got, want) {
			t.Errorf("%.60q: Decode %#v, want %#v", doc, got, want)
		}
	}
	for _, bad := range []string{``, `{`, `[1,]`, `{"a"}`, `{"a":}`, `01`, `1.`, `1e`, `-`, `tru`, `"abc`, "\"a\x01\"", `{} {}`, `{"a":1,}`, `nul`, `[1 2]`} {
		if _, werr := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(bad))); werr == nil {
			t.Fatalf("%q: UnmarshalJSON accepts it; the case is wrong", bad)
		}
		if _, err := Decode([]byte(bad)); err == nil {
			t.Errorf("%q: Decode accepted it", bad)
		}
	}
}
