package i18n

import (
	"fmt"
	"io/fs"
	"maps"
	"regexp"

	"github.com/BurntSushi/toml"
)

type slot struct {
	name       string
	start, end int
}

type message struct {
	text  string
	slots []slot
}

func readTranslations(fsys fs.FS, filename string, pattern *regexp.Regexp) (map[string]message, error) {
	var table map[string]any
	if _, err := toml.DecodeFS(fsys, filename, &table); err != nil {
		return nil, fmt.Errorf("load %s: %w", filename, err)
	}
	flat := make(map[string]message)
	if err := flatten(flat, "", table, pattern); err != nil {
		return nil, fmt.Errorf("load %s: %w", filename, err)
	}
	return flat, nil
}

func flatten(flat map[string]message, prefix string, table map[string]any, pattern *regexp.Regexp) error {
	for name, raw := range table {
		key := prefix + name
		switch value := raw.(type) {
		case map[string]any:
			if err := flatten(flat, key+".", value, pattern); err != nil {
				return err
			}
		case string:
			if _, ok := flat[key]; ok {
				return fmt.Errorf("duplicate flattened translation key %q", key)
			}
			msg := message{text: value}
			for _, span := range pattern.FindAllStringIndex(value, -1) {
				start, end := span[0], span[1]
				msg.slots = append(msg.slots, slot{name: value[start+1 : end-1], start: start, end: end})
			}
			flat[key] = msg
		default:
			return fmt.Errorf("translation %q must be a string", key)
		}
	}
	return nil
}

func validateTranslations(zh, en map[string]message) error {
	for key, zhMsg := range zh {
		enMsg, ok := en[key]
		if !ok {
			return fmt.Errorf("translation %q is missing from %s", key, En)
		}
		if !maps.Equal(placeholderNames(zhMsg), placeholderNames(enMsg)) {
			return fmt.Errorf("translation %q has different placeholders between %s and %s", key, ZhHans, En)
		}
	}
	for key := range en {
		if _, ok := zh[key]; !ok {
			return fmt.Errorf("translation %q is missing from %s", key, ZhHans)
		}
	}
	return nil
}

func placeholderNames(msg message) map[string]struct{} {
	var names map[string]struct{}
	if len(msg.slots) > 0 {
		names = make(map[string]struct{}, len(msg.slots))
		for _, slot := range msg.slots {
			names[slot.name] = struct{}{}
		}
	}
	return names
}
