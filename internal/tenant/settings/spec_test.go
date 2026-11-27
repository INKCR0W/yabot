package settings

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

type specFixture struct {
	Flag  bool    `json:"flag"`
	Count int64   `json:"count,omitempty"`
	Mode  string  `json:"mode"`
	Delay Seconds `json:"delay"`
}

func specFixtureDefinition() Spec {
	minimum, maximum := int64(1), int64(7)
	secondsMin, secondsMax := int64(30), int64(90)
	return Spec{
		Feature: "example",
		Version: 1,
		Groups:  []GroupMeta{{ID: "basic"}},
		Fields: []Field{
			{Key: "flag", Type: FieldBool, Group: "basic", Default: json.RawMessage("true")},
			{Key: "count", Type: FieldInt, Group: "basic", Default: json.RawMessage("3"), Min: &minimum, Max: &maximum, Step: 2},
			{Key: "mode", Type: FieldEnum, Group: "basic", Default: json.RawMessage(`"normal"`), Options: []Option{{Value: "normal"}, {Value: "strict"}}},
			{Key: "delay", Type: FieldDuration, Group: "basic", Default: json.RawMessage("60"), Min: &secondsMin, Max: &secondsMax, Step: 30},
		},
	}
}

func TestRegisteredFieldStrictScalarValues(t *testing.T) {
	spec := compileSpec(reflect.TypeFor[specFixture](), specFixtureDefinition())
	for _, tc := range []struct {
		field string
		raw   string
		want  any
	}{
		{"flag", "true", true},
		{"flag", " \tfalse\n", false},
		{"flag", `"true"`, nil},
		{"flag", "1", nil},
		{"flag", "null", nil},
		{"flag", "", nil},
		{"flag", "tru", nil},
		{"flag", "true true", nil},
		{"flag", "true]", nil},
		{"flag", "true,", nil},
		{"flag", "[]", nil},
		{"flag", "{}", nil},
		{"count", "1", int64(1)},
		{"count", "7", int64(7)},
		{"count", "0", nil},
		{"count", "-1", nil},
		{"count", "8", nil},
		{"count", "2", nil},
		{"count", "1.0", nil},
		{"count", "1e3", nil},
		{"count", "9223372036854775808", nil},
		{"count", "-9223372036854775809", nil},
		{"count", `"1"`, nil},
		{"count", "01", nil},
		{"count", "null", nil},
		{"mode", `"normal"`, "normal"},
		{"mode", `"strict"`, "strict"},
		{"mode", `"invalid"`, nil},
		{"mode", `"Normal"`, nil},
		{"mode", `"normal`, nil},
		{"mode", `"normal" ""`, nil},
		{"mode", "null", nil},
		{"delay", "30", Seconds(30)},
		{"delay", "90", Seconds(90)},
		{"delay", "29", nil},
		{"delay", "0", nil},
		{"delay", "91", nil},
		{"delay", "31", nil},
		{"delay", "30.0", nil},
		{"delay", "1e3", nil},
		{"delay", "9223372036854775808", nil},
		{"delay", `"30"`, nil},
		{"delay", "null", nil},
	} {
		t.Run(tc.field+"/"+tc.raw, func(t *testing.T) {
			got, err := spec.fields[tc.field].decode(json.RawMessage(tc.raw))
			if tc.want == nil {
				if err == nil {
					t.Fatalf("decode(%q) unexpectedly accepted %v", tc.raw, got.Interface())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Interface() != tc.want {
				t.Errorf("decode(%q) = %v (%T), want %v (%T)", tc.raw, got.Interface(), got.Interface(), tc.want, tc.want)
			}
		})
	}
}

func TestRegisteredFieldIntegerStepUsesFullRange(t *testing.T) {
	minimum, maximum := int64(math.MinInt64), int64(math.MaxInt64)
	for _, tc := range []struct {
		name string
		min  *int64
		max  *int64
		step int64
		raw  string
		want bool
	}{
		{"full range aligned", &minimum, &maximum, 3, "9223372036854775807", true},
		{"full range misaligned", &minimum, &maximum, 3, "9223372036854775806", false},
		{"negative aligned without minimum", nil, nil, 2, "-4", true},
		{"negative misaligned without minimum", nil, nil, 2, "-3", false},
		{"minimum integer without minimum", nil, nil, 2, "-9223372036854775808", true},
		{"maximum integer without maximum", nil, nil, 1, "9223372036854775807", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := specFixtureDefinition()
			field := &definition.Fields[1]
			field.Min, field.Max, field.Step = tc.min, tc.max, tc.step
			field.Default = json.RawMessage("0")
			if tc.min != nil {
				field.Default = json.RawMessage("-9223372036854775808")
			}
			spec := compileSpec(reflect.TypeFor[specFixture](), definition)
			if _, err := spec.fields["count"].decode(json.RawMessage(tc.raw)); (err == nil) != tc.want {
				t.Errorf("decode(%q) returned %v, want valid=%v", tc.raw, err, tc.want)
			}
		})
	}
}

func TestRegisteredFieldIntegerUsesGoWidth(t *testing.T) {
	type narrowCount int8
	typ := changeFixtureStructField(1, func(field *reflect.StructField) { field.Type = reflect.TypeFor[narrowCount]() })
	definition := specFixtureDefinition()
	definition.Fields[1].Min = nil
	definition.Fields[1].Max = nil
	definition.Fields[1].Step = 0
	spec := compileSpec(typ, definition)
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"-129", false},
		{"-128", true},
		{"127", true},
		{"128", false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			if _, err := spec.fields["count"].decode(json.RawMessage(tc.raw)); (err == nil) != tc.want {
				t.Errorf("decode(%q) returned %v, want valid=%v", tc.raw, err, tc.want)
			}
		})
	}
}

func TestRegisteredFieldRejectsInvalidUTF8(t *testing.T) {
	definition := specFixtureDefinition()
	definition.Fields[2].Options = append(definition.Fields[2].Options, Option{Value: "\uFFFD"})
	spec := compileSpec(reflect.TypeFor[specFixture](), definition)
	if value, err := spec.fields["mode"].decode(json.RawMessage{'"', 0xFF, '"'}); err == nil {
		t.Errorf("invalid UTF-8 accepted as %q", value.String())
	}
}

func TestCompileSpecRejectsInconsistencies(t *testing.T) {
	type namedBool bool
	type namedString string
	condition := func(clauses ...Clause) func(*Spec) {
		return func(s *Spec) { s.Fields[3].VisibleIf = &Cond{All: clauses} }
	}
	fieldType := func(index int, typ reflect.Type) reflect.Type {
		return changeFixtureStructField(index, func(field *reflect.StructField) { field.Type = typ })
	}
	fieldTag := func(index int, tag reflect.StructTag) reflect.Type {
		return changeFixtureStructField(index, func(field *reflect.StructField) { field.Tag = tag })
	}
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		edit func(*Spec)
	}{
		{name: "not struct", typ: reflect.TypeFor[*specFixture]()},
		{name: "empty feature", edit: func(s *Spec) { s.Feature = "" }},
		{name: "tag missing", typ: fieldTag(0, "")},
		{name: "tag is ignored", typ: fieldTag(0, `json:"-"`)},
		{name: "tag omits name", typ: fieldTag(0, `json:",omitempty"`)},
		{name: "tag encodes a string", typ: fieldTag(0, `json:"flag,string"`)},
		{name: "duplicate tags", typ: fieldTag(1, `json:"flag"`)},
		{name: "mismatched key", edit: func(s *Spec) { s.Fields[0].Key = "wrong" }},
		{name: "undeclared tag", edit: func(s *Spec) { s.Fields = s.Fields[1:] }},
		{name: "duplicate key", edit: func(s *Spec) { s.Fields = append(s.Fields, s.Fields[2]) }},
		{name: "unknown group", edit: func(s *Spec) { s.Fields[1].Group = "other" }},
		{name: "duplicate group", edit: func(s *Spec) { s.Groups = append(s.Groups, s.Groups[0]) }},
		{name: "unnamed group", edit: func(s *Spec) { s.Groups[0].ID = "" }},
		{name: "nonscalar field", typ: fieldType(0, reflect.TypeFor[*bool]())},
		{name: "bool requires bool", typ: fieldType(0, reflect.TypeFor[namedBool]())},
		{name: "int requires signed int", typ: fieldType(1, reflect.TypeFor[uint64]())},
		{name: "enum requires string", typ: fieldType(2, reflect.TypeFor[namedString]())},
		{name: "duration requires seconds", typ: fieldType(3, reflect.TypeFor[int64]())},
		{name: "reserved field type", edit: func(s *Spec) { s.Fields[2].Type = FieldText }},
		{name: "malformed default", edit: func(s *Spec) { s.Fields[0].Default = json.RawMessage("tru") }},
		{name: "wrong default type", edit: func(s *Spec) { s.Fields[0].Default = json.RawMessage("1") }},
		{name: "out of range default", edit: func(s *Spec) { s.Fields[1].Default = json.RawMessage("9") }},
		{name: "misaligned default", edit: func(s *Spec) { s.Fields[3].Default = json.RawMessage("45") }},
		{name: "unlisted enum default", edit: func(s *Spec) { s.Fields[2].Default = json.RawMessage(`"other"`) }},
		{name: "enum has no options", edit: func(s *Spec) { s.Fields[2].Options = nil }},
		{name: "enum has duplicate options", edit: func(s *Spec) { s.Fields[2].Options = append(s.Fields[2].Options, s.Fields[2].Options[0]) }},
		{name: "reversed bounds", edit: func(s *Spec) { s.Fields[1].Min, s.Fields[1].Max = s.Fields[1].Max, s.Fields[1].Min }},
		{name: "negative step", edit: func(s *Spec) { s.Fields[1].Step = -1 }},
		{name: "enum has step", edit: func(s *Spec) { s.Fields[2].Step = 2 }},
		{name: "bool has bound", edit: func(s *Spec) { s.Fields[0].Max = new(int64(1)) }},
		{name: "bool has options", edit: func(s *Spec) { s.Fields[0].Options = s.Fields[2].Options }},
		{name: "visibility references missing field", edit: condition(Clause{Field: "other", Eq: json.RawMessage("true")})},
		{name: "visibility lacks all and any", edit: func(s *Spec) { s.Fields[3].VisibleIf = &Cond{} }},
		{name: "visibility has both all and any", edit: func(s *Spec) {
			clauses := []Clause{{Field: "flag", Eq: json.RawMessage("true")}}
			s.Fields[3].VisibleIf = &Cond{All: clauses, Any: clauses}
		}},
		{name: "visibility lacks operator", edit: condition(Clause{Field: "flag"})},
		{name: "visibility has multiple operators", edit: condition(Clause{Field: "flag", Eq: json.RawMessage("true"), Ne: json.RawMessage("false")})},
		{name: "visibility has invalid comparison", edit: condition(Clause{Field: "flag", Eq: json.RawMessage(`"true"`)})},
		{name: "visibility has invalid in member", edit: condition(Clause{Field: "flag", In: []json.RawMessage{json.RawMessage("false"), json.RawMessage("null")}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := specFixtureDefinition()
			if tc.edit != nil {
				tc.edit(&definition)
			}
			typ := tc.typ
			if typ == nil {
				typ = reflect.TypeFor[specFixture]()
			}
			defer func() {
				if recover() == nil {
					t.Error("compileSpec did not panic")
				}
			}()
			compileSpec(typ, definition)
		})
	}
}

func TestRegisteredFieldEmptyVisibilityArrays(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condition Cond
		visible   bool
		wire      string
	}{
		{"all", Cond{All: []Clause{}}, true, `{"all":[]}`},
		{"any", Cond{Any: []Clause{}}, false, `{"any":[]}`},
		{"in", Cond{All: []Clause{{Field: "mode", In: []json.RawMessage{}}}}, false, `{"all":[{"field":"mode","in":[]}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			definition := specFixtureDefinition()
			definition.Fields[3].VisibleIf = &tc.condition
			spec := compileSpec(reflect.TypeFor[specFixture](), definition)
			field := spec.fields["delay"]
			if got := field.visible(reflect.ValueOf(spec.defaults)); got != tc.visible {
				t.Errorf("visible = %v, want %v", got, tc.visible)
			}
			raw, err := json.Marshal(field.VisibleIf)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != tc.wire {
				t.Errorf("condition JSON = %s, want %s", raw, tc.wire)
			}
		})
	}
}

func TestRegisteredSpecOwnsMutableMetadata(t *testing.T) {
	definition := specFixtureDefinition()
	definition.Fields[3].VisibleIf = &Cond{
		All: []Clause{{Field: "mode", In: []json.RawMessage{json.RawMessage(`"normal"`)}}},
	}
	spec := compileSpec(reflect.TypeFor[specFixture](), definition)

	definition.Groups[0].ID = "changed"
	definition.Fields[0].Default[0] = 'x'
	*definition.Fields[1].Min = 99
	*definition.Fields[1].Max = 0
	definition.Fields[2].Options[0].Value = "changed"
	definition.Fields[3].VisibleIf.All[0].In[0][1] = 'x'
	definition.Fields[3].VisibleIf.All[0].Field = "changed"

	if spec.Groups[0].ID != "basic" || string(spec.Fields[0].Default) != "true" ||
		*spec.Fields[1].Min != 1 || *spec.Fields[1].Max != 7 ||
		spec.Fields[2].Options[0].Value != "normal" {
		t.Fatalf("registered metadata changed: %+v", spec.Spec)
	}
	clause := spec.Fields[3].VisibleIf.All[0]
	if clause.Field != "mode" || string(clause.In[0]) != `"normal"` {
		t.Fatalf("registered condition changed: %+v", clause)
	}
}

func changeFixtureStructField(index int, mutate func(*reflect.StructField)) reflect.Type {
	typ := reflect.TypeFor[specFixture]()
	fields := make([]reflect.StructField, typ.NumField())
	for i := range fields {
		fields[i] = typ.Field(i)
	}
	mutate(&fields[index])
	return reflect.StructOf(fields)
}
