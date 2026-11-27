package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

type registeredSpec struct {
	Spec
	typ      reflect.Type
	defaults any
	fields   map[string]*registeredField
}

type registeredField struct {
	Field
	index     int
	typ       reflect.Type
	options   map[string]struct{}
	condition *fieldCondition
}

type fieldCondition struct {
	any     bool
	clauses []fieldClause
}

type fieldClause struct {
	index  int
	negate bool
	values []reflect.Value
}

func compileSpec(typ reflect.Type, spec Spec) *registeredSpec {
	if typ.Kind() != reflect.Struct {
		panic("settings: registered settings must be a struct")
	}
	if spec.Feature == "" {
		panic("settings: feature name is required")
	}
	fieldIndexes := jsonFieldIndexes(typ)
	spec.Groups = slices.Clone(spec.Groups)
	groups := make(map[string]struct{}, len(spec.Groups))
	for _, group := range spec.Groups {
		if _, exists := groups[group.ID]; group.ID == "" || exists {
			panic(fmt.Sprintf("settings: invalid or duplicate group %q", group.ID))
		}
		groups[group.ID] = struct{}{}
	}

	spec.Fields = slices.Clone(spec.Fields)
	registered := &registeredSpec{Spec: spec, typ: typ, fields: make(map[string]*registeredField, len(spec.Fields))}
	defaults := reflect.New(typ).Elem()
	for i := range registered.Fields {
		field := cloneField(registered.Fields[i])
		registered.Fields[i] = field
		index, ok := fieldIndexes[field.Key]
		if !ok {
			panic(fmt.Sprintf("settings: field %q has no matching JSON tag", field.Key))
		}
		if _, exists := registered.fields[field.Key]; exists {
			panic(fmt.Sprintf("settings: duplicate field %q", field.Key))
		}
		if _, ok := groups[field.Group]; !ok {
			panic(fmt.Sprintf("settings: field %q references unknown group %q", field.Key, field.Group))
		}

		compiled := compileField(field, index, typ.Field(index).Type)
		value, err := compiled.decode(field.Default)
		if err != nil {
			panic(fmt.Sprintf("settings: field %q has an invalid default: %v", field.Key, err))
		}
		defaults.Field(index).Set(value)
		registered.fields[field.Key] = compiled
	}
	for key := range fieldIndexes {
		if _, ok := registered.fields[key]; !ok {
			panic(fmt.Sprintf("settings: JSON field %q has no declaration", key))
		}
	}

	for _, field := range registered.Fields {
		if field.VisibleIf != nil {
			registered.fields[field.Key].condition = compileCondition(field, registered.fields)
		}
	}
	registered.defaults = defaults.Interface()
	return registered
}

func jsonFieldIndexes(typ reflect.Type) map[string]int {
	fields := make(map[string]int)
	for i := range typ.NumField() {
		field := typ.Field(i)
		switch field.Type.Kind() {
		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		default:
			panic(fmt.Sprintf("settings: struct field %q must be scalar", field.Name))
		}
		tag, tagged := field.Tag.Lookup("json")
		if !field.IsExported() || !tagged || tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if name == "" || slices.Contains(strings.Split(options, ","), "string") {
			panic(fmt.Sprintf("settings: struct field %q requires an explicit JSON key and an unquoted scalar", field.Name))
		}
		if _, exists := fields[name]; exists {
			panic(fmt.Sprintf("settings: duplicate JSON field %q", name))
		}
		fields[name] = i
	}
	return fields
}

func cloneField(field Field) Field {
	field.Default = bytes.Clone(field.Default)
	field.Options = slices.Clone(field.Options)
	if field.Min != nil {
		minimum := *field.Min
		field.Min = &minimum
	}
	if field.Max != nil {
		maximum := *field.Max
		field.Max = &maximum
	}
	if field.VisibleIf != nil {
		field.VisibleIf = &Cond{
			All: cloneClauses(field.VisibleIf.All),
			Any: cloneClauses(field.VisibleIf.Any),
		}
	}
	return field
}

func cloneClauses(clauses []Clause) []Clause {
	cloned := slices.Clone(clauses)
	for i := range cloned {
		clause := &cloned[i]
		clause.Eq = bytes.Clone(clause.Eq)
		clause.Ne = bytes.Clone(clause.Ne)
		clause.In = slices.Clone(clause.In)
		for j := range clause.In {
			clause.In[j] = bytes.Clone(clause.In[j])
		}
	}
	return cloned
}

func compileField(field Field, index int, typ reflect.Type) *registeredField {
	var matching bool
	switch field.Type {
	case FieldBool:
		matching = typ == reflect.TypeFor[bool]()
	case FieldInt:
		matching = typ.Kind() >= reflect.Int && typ.Kind() <= reflect.Int64
	case FieldEnum:
		matching = typ == reflect.TypeFor[string]()
	case FieldDuration:
		matching = typ == reflect.TypeFor[Seconds]()
	}
	if !matching {
		panic(fmt.Sprintf("settings: field %q type %q does not match %v", field.Key, field.Type, typ))
	}
	if field.Min != nil && field.Max != nil && *field.Min > *field.Max {
		panic(fmt.Sprintf("settings: field %q has a minimum above its maximum", field.Key))
	}
	if field.Step < 0 {
		panic(fmt.Sprintf("settings: field %q has a negative step", field.Key))
	}
	numeric := field.Type == FieldInt || field.Type == FieldDuration
	if !numeric && (field.Min != nil || field.Max != nil || field.Step != 0) {
		panic(fmt.Sprintf("settings: field %q has numeric constraints but type %q", field.Key, field.Type))
	}
	if field.Type != FieldEnum && len(field.Options) > 0 {
		panic(fmt.Sprintf("settings: field %q has options but type %q", field.Key, field.Type))
	}
	compiled := &registeredField{Field: field, index: index, typ: typ}
	if field.Type == FieldEnum {
		if len(field.Options) == 0 {
			panic(fmt.Sprintf("settings: enum field %q requires options", field.Key))
		}
		compiled.options = make(map[string]struct{}, len(field.Options))
		for _, option := range field.Options {
			if _, exists := compiled.options[option.Value]; exists {
				panic(fmt.Sprintf("settings: enum field %q has duplicate options", field.Key))
			}
			compiled.options[option.Value] = struct{}{}
		}
	}
	return compiled
}

func compileCondition(field Field, fields map[string]*registeredField) *fieldCondition {
	condition := field.VisibleIf
	if (condition.All == nil) == (condition.Any == nil) {
		panic(fmt.Sprintf("settings: field %q requires exactly one visibility expression", field.Key))
	}
	clauses := condition.All
	if condition.Any != nil {
		clauses = condition.Any
	}
	compiled := &fieldCondition{any: condition.Any != nil, clauses: make([]fieldClause, 0, len(clauses))}
	for _, clause := range clauses {
		referenced, ok := fields[clause.Field]
		if !ok {
			panic(fmt.Sprintf("settings: visibility for field %q references unknown field %q", field.Key, clause.Field))
		}
		source := clause.In
		operators := 0
		if clause.In != nil {
			operators++
		}
		if clause.Eq != nil {
			source = []json.RawMessage{clause.Eq}
			operators++
		}
		if clause.Ne != nil {
			source = []json.RawMessage{clause.Ne}
			operators++
		}
		if operators != 1 {
			panic(fmt.Sprintf("settings: visibility clause for field %q requires exactly one operator", field.Key))
		}

		values := make([]reflect.Value, 0, len(source))
		for _, raw := range source {
			value, err := referenced.decodeTyped(raw)
			if err != nil {
				panic(fmt.Sprintf("settings: visibility clause for field %q has an invalid comparison: %v", field.Key, err))
			}
			values = append(values, value)
		}
		compiled.clauses = append(compiled.clauses, fieldClause{
			index: referenced.index, negate: clause.Ne != nil, values: values,
		})
	}
	return compiled
}

func (f *registeredField) decode(raw json.RawMessage) (reflect.Value, error) {
	value, err := f.decodeTyped(raw)
	if err != nil {
		return reflect.Value{}, err
	}
	switch f.Type {
	case FieldInt, FieldDuration:
		number := value.Int()
		if f.Min != nil && number < *f.Min || f.Max != nil && number > *f.Max {
			return reflect.Value{}, errors.New("number is out of range")
		}
		if f.Step > 0 {
			var base int64
			if f.Min != nil {
				base = *f.Min
			}
			// The distance between two signed integers can exceed MaxInt64.
			distance := uint64(max(number, base)) - uint64(min(number, base))
			if distance%uint64(f.Step) != 0 {
				return reflect.Value{}, errors.New("number does not match the step")
			}
		}
	case FieldEnum:
		if _, ok := f.options[value.String()]; !ok {
			return reflect.Value{}, errors.New("enum value is not an option")
		}
	}
	return value, nil
}

func (f *registeredField) decodeTyped(raw json.RawMessage) (reflect.Value, error) {
	if !utf8.Valid(raw) {
		return reflect.Value{}, errors.New("expected valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return reflect.Value{}, err
	}
	value := reflect.New(f.typ).Elem()
	switch f.Type {
	case FieldBool:
		boolean, ok := token.(bool)
		if !ok {
			return reflect.Value{}, errors.New("expected a JSON boolean")
		}
		value.SetBool(boolean)
	case FieldInt, FieldDuration:
		number, ok := token.(json.Number)
		if !ok {
			return reflect.Value{}, errors.New("expected a JSON integer")
		}
		integer, err := strconv.ParseInt(number.String(), 10, f.typ.Bits())
		if err != nil {
			return reflect.Value{}, fmt.Errorf("invalid integer: %w", err)
		}
		value.SetInt(integer)
	case FieldEnum:
		str, ok := token.(string)
		if !ok {
			return reflect.Value{}, errors.New("expected a JSON string")
		}
		value.SetString(str)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return reflect.Value{}, errors.New("expected exactly one JSON value")
	}
	return value, nil
}

func (f *registeredField) visible(candidate reflect.Value) bool {
	if f.condition == nil {
		return true
	}
	for _, clause := range f.condition.clauses {
		field := candidate.Field(clause.index)
		matches := slices.ContainsFunc(clause.values, field.Equal) != clause.negate
		if matches == f.condition.any {
			return matches
		}
	}
	return !f.condition.any
}
