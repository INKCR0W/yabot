package static

import "strings"

const fullQuality = 1000

func selectEncoding(values []string, hasBr, hasGzip bool) string {
	qualities := make(map[string]int, 4)
	for _, value := range values {
		for item := range strings.SplitSeq(value, ",") {
			name, parameter, hasParameter := strings.Cut(item, ";")
			name = strings.ToLower(strings.TrimSpace(name))
			switch name {
			case "br", "gzip", "identity", "*":
			default:
				continue
			}

			quality := fullQuality
			if hasParameter {
				quality = parseQuality(parameter)
			}
			if previous, exists := qualities[name]; !exists || quality < previous {
				qualities[name] = quality
			}
		}
	}

	bestEncoding := ""
	bestQuality := -1
	for _, candidate := range [...]struct {
		name      string
		available bool
	}{
		{"br", hasBr},
		{"gzip", hasGzip},
		{"identity", true},
	} {
		if !candidate.available {
			continue
		}
		quality, explicit := qualities[candidate.name]
		if !explicit {
			quality, explicit = qualities["*"]
		}
		// Implicit identity is the fallback if no preferred representation qualifies.
		if explicit && quality > 0 && (bestQuality < 0 || quality > bestQuality) {
			bestQuality = quality
			bestEncoding = candidate.name
		}
	}
	if bestEncoding == "identity" {
		return ""
	}
	return bestEncoding
}

func parseQuality(parameter string) int {
	key, value, found := strings.Cut(parameter, "=")
	if !found || !strings.EqualFold(strings.TrimSpace(key), "q") {
		return 0
	}
	whole, fraction, _ := strings.Cut(strings.TrimSpace(value), ".")
	if (whole != "0" && whole != "1") || len(fraction) > 3 {
		return 0
	}
	quality := 0
	for i, place := 0, 100; i < len(fraction); i, place = i+1, place/10 {
		digit := fraction[i]
		if digit < '0' || digit > '9' {
			return 0
		}
		quality += int(digit-'0') * place
	}
	if whole == "1" {
		if quality != 0 {
			return 0
		}
		return fullQuality
	}
	return quality
}
