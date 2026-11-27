package settings

import "encoding/json"

type Schema struct {
	SchemaVersion int            `json:"schema_version"`
	Specs         []RenderedSpec `json:"specs"`
}

type RenderedSpec struct {
	Feature string                     `json:"feature"`
	Title   string                     `json:"title"`
	Groups  []RenderedGroup            `json:"groups"`
	Fields  []RenderedField            `json:"fields"`
	Values  map[string]json.RawMessage `json:"values"`
	ETag    string                     `json:"etag"`
}

type RenderedGroup struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type RenderedField struct {
	Key       string           `json:"key"`
	Type      FieldType        `json:"type"`
	Label     string           `json:"label"`
	Help      string           `json:"help,omitempty"`
	Group     string           `json:"group"`
	Default   json.RawMessage  `json:"default"`
	Min       *int64           `json:"min,omitempty"`
	Max       *int64           `json:"max,omitempty"`
	Step      int64            `json:"step,omitempty"`
	Options   []RenderedOption `json:"options,omitempty"`
	VisibleIf *Cond            `json:"visible_if,omitempty"`
}

type RenderedOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type LocalizedFieldResult struct {
	Code    FieldCode `json:"code"`
	Message string    `json:"message"`
}

// Render requires Freeze and returns independently owned schema and values.
func (r *Registry) Render(chatID int64, lang string) Schema {
	r.requireFrozen()
	rows := r.readSnapshot(chatID)
	response := Schema{
		SchemaVersion: 1,
		Specs:         make([]RenderedSpec, 0, len(r.specs)),
	}
	for _, spec := range r.specs {
		current := rows.current(spec, chatID)
		rendered := RenderedSpec{
			Feature: spec.Feature,
			Title:   r.catalog.T(lang, spec.Title, nil),
			Groups:  make([]RenderedGroup, 0, len(spec.Groups)),
			Fields:  make([]RenderedField, 0, len(spec.Fields)),
			Values:  spec.values(current.value),
			ETag:    current.etag,
		}
		for _, group := range spec.Groups {
			rendered.Groups = append(rendered.Groups, RenderedGroup{
				ID:    group.ID,
				Title: r.catalog.T(lang, group.Title, nil),
			})
		}
		for _, field := range spec.Fields {
			rendered.Fields = append(rendered.Fields, r.renderField(lang, field))
		}
		response.Specs = append(response.Specs, rendered)
	}
	return response
}

func (r *Registry) renderField(lang string, field Field) RenderedField {
	field = cloneField(field)
	result := RenderedField{
		Key:       field.Key,
		Type:      field.Type,
		Label:     r.catalog.T(lang, field.Label, nil),
		Group:     field.Group,
		Default:   field.Default,
		VisibleIf: field.VisibleIf,
	}
	if field.Help != "" {
		result.Help = r.catalog.T(lang, field.Help, nil)
	}
	switch field.Type {
	case FieldInt, FieldDuration:
		result.Min = field.Min
		result.Max = field.Max
		result.Step = field.Step
	case FieldEnum:
		result.Options = make([]RenderedOption, 0, len(field.Options))
		for _, option := range field.Options {
			result.Options = append(result.Options, RenderedOption{
				Value: option.Value,
				Label: r.catalog.T(lang, option.Label, nil),
			})
		}
	}
	return result
}

// LocalizeFields requires Freeze and projects the failures for the console's 422 response.
func (r *Registry) LocalizeFields(lang string, result PatchResult) map[string]LocalizedFieldResult {
	r.requireFrozen()
	fields := make(map[string]LocalizedFieldResult)
	for key, field := range result.Fields {
		if field.Code == CodeOK {
			continue
		}
		fields[key] = LocalizedFieldResult{
			Code:    field.Code,
			Message: r.catalog.T(lang, field.Key, field.Args),
		}
	}
	return fields
}
