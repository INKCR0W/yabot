package settings

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

func (s *registeredSpec) values(value any) map[string]json.RawMessage {
	fields := make(map[string]json.RawMessage, len(s.fields))
	reflected := reflect.ValueOf(value)
	for key, field := range s.fields {
		v := reflected.Field(field.index)
		switch field.Type {
		case FieldBool:
			fields[key] = strconv.AppendBool(nil, v.Bool())
		case FieldInt, FieldDuration:
			fields[key] = strconv.AppendInt(nil, v.Int(), 10)
		case FieldEnum:
			encoded, err := json.Marshal(v.String())
			if err != nil {
				panic(fmt.Errorf("encode settings enum: %w", err))
			}
			fields[key] = encoded
		}
	}
	return fields
}

type fieldChange struct {
	Old json.RawMessage `json:"old"`
	New json.RawMessage `json:"new"`
}

func encodeChange(spec *registeredSpec, previous any, values map[string]json.RawMessage) ([]byte, []byte, error) {
	data, err := json.Marshal(values)
	if err != nil {
		return nil, nil, fmt.Errorf("encode settings: %w", err)
	}
	diff := make(map[string]fieldChange)
	for key, old := range spec.values(previous) {
		if next := values[key]; !bytes.Equal(old, next) {
			diff[key] = fieldChange{Old: old, New: next}
		}
	}
	encoded, err := json.Marshal(diff)
	if err != nil {
		return nil, nil, fmt.Errorf("encode settings audit diff: %w", err)
	}
	return data, encoded, nil
}
