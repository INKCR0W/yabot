package static

import (
	"math/rand/v2"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestSelectEncoding(t *testing.T) {
	tests := []struct {
		name     string
		headers  []string
		br, gzip bool
		want     string
	}{
		{"missing header", nil, true, true, ""},
		{"empty header", []string{""}, true, true, ""},
		{"brotli", []string{"br"}, true, true, "br"},
		{"gzip", []string{"gzip"}, true, true, "gzip"},
		{"identity", []string{"identity"}, true, true, ""},
		{"refuses brotli", []string{"br;q=0, gzip;q=0.1"}, true, true, "gzip"},
		{"refuses only available compression", []string{"br;q=0"}, true, false, ""},
		{"higher quality gzip", []string{"gzip;q=0.5, br;q=0.4"}, true, true, "gzip"},
		{"higher quality brotli", []string{"br;q=0.500, gzip;q=0.499"}, true, true, "br"},
		{"brotli wins tie", []string{"gzip;q=0.50, br;q=0.500"}, true, true, "br"},
		{"gzip wins tie with identity", []string{"identity;q=0.5, gzip;q=0.5"}, true, true, "gzip"},
		{"explicit identity outranks compression", []string{"br;q=0.4, identity;q=0.5"}, true, true, ""},
		{"wildcard", []string{"*"}, true, true, "br"},
		{"wildcard rejects all", []string{"*;q=0"}, true, true, ""},
		{"wildcard skips refused brotli", []string{"*;q=0.8, br;q=0"}, true, true, "gzip"},
		{"wildcard quality for unlisted identity", []string{"*;q=0.8, br;q=0.5"}, true, false, ""},
		{"explicit brotli overrides wildcard", []string{"*;q=0, br;q=0.5"}, true, true, "br"},
		{"refuses identity without compression", []string{"identity;q=0"}, false, false, ""},
		{"refuses identity with compression", []string{"identity;q=0, br;q=0.001"}, true, true, "br"},
		{"case and space", []string{" \tGZIP ; Q = 0.2 ,  Br\t; q=1.000 "}, true, true, "br"},
		{"trailing decimal point", []string{"br;q=1., gzip;q=0.5"}, true, true, "br"},
		{"invalid number", []string{"br;q=oops, gzip;q=0.4"}, true, true, "gzip"},
		{"out of range", []string{"br;q=1.001, gzip;q=0.4"}, true, true, "gzip"},
		{"too many fractional digits", []string{"br;q=0.9000, gzip;q=0.4"}, true, true, "gzip"},
		{"no leading digit", []string{"br;q=.9, gzip;q=0.4"}, true, true, "gzip"},
		{"unknown parameter", []string{"br;level=9, gzip;q=0.4"}, true, true, "gzip"},
		{"extra parameter", []string{"br;q=0.9;level=9, gzip;q=0.4"}, true, true, "gzip"},
		{"only gzip available", []string{"br, gzip;q=0.3"}, false, true, "gzip"},
		{"unavailable preference", []string{"br, zstd"}, false, true, ""},
		{"no compressed variants", []string{"br, gzip"}, false, false, ""},
		{"multiple field lines", []string{"br;q=0.3", "gzip;q=0.5"}, true, true, "gzip"},
		{"duplicate refusal wins", []string{"br;q=0", "br, gzip;q=0.5"}, true, true, "gzip"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := selectEncoding(test.headers, test.br, test.gzip)
			if got != test.want {
				t.Fatalf("selectEncoding(%q, %t, %t) = %q, want %q", test.headers, test.br, test.gzip, got, test.want)
			}
		})
	}
}

func TestSelectEncodingAgainstReference(t *testing.T) {
	random := rand.New(rand.NewPCG(14, 197))
	qPattern := regexp.MustCompile(`(?i)^\s*q\s*=\s*(0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?)\s*$`)
	names := []string{"br", "BR", "gzip", "GZip", "identity", "IDENTITY", "*", "deflate", "zstd", ""}
	parameters := []string{"", ";q=0", ";q=0.001", ";Q=0.4", ";q=0.40", "; q = 0.5 ", ";q=0.999",
		";q=1", ";q=1.", ";q=1.000", ";q=1.01", ";q=2", ";q=-0.1", ";q=.5", ";q=0.1234",
		";q=NaN", ";q=Inf", ";q=1e0", ";q=", ";level=3", ";q=0.9;q=0.2"}
	whitespace := []string{"", " ", "\t", " \t"}
	for range 1200 {
		headers := make([]string, random.IntN(4))
		for i := range headers {
			items := make([]string, random.IntN(6))
			for j := range items {
				items[j] = whitespace[random.IntN(len(whitespace))] +
					names[random.IntN(len(names))] +
					parameters[random.IntN(len(parameters))] +
					whitespace[random.IntN(len(whitespace))]
			}
			headers[i] = strings.Join(items, ",")
		}
		for _, hasBr := range []bool{false, true} {
			for _, hasGzip := range []bool{false, true} {
				want := referenceEncoding(t, qPattern, headers, hasBr, hasGzip)
				if got := selectEncoding(headers, hasBr, hasGzip); got != want {
					t.Fatalf("selectEncoding(%q, %t, %t) = %q, want %q", headers, hasBr, hasGzip, got, want)
				}
			}
		}
	}
}

func referenceEncoding(t *testing.T, qPattern *regexp.Regexp, headers []string, hasBr, hasGzip bool) string {
	var available []string
	if hasBr {
		available = append(available, "br")
	}
	if hasGzip {
		available = append(available, "gzip")
	}
	available = append(available, "identity")
	type option struct {
		name    string
		quality float64
	}
	wildcard := referenceQuality(t, qPattern, headers, "*")
	var options []option
	for _, name := range available {
		quality := referenceQuality(t, qPattern, headers, name)
		if quality == -1 {
			quality = wildcard
		}
		if quality > 0 {
			options = append(options, option{name, quality})
		}
	}
	sort.SliceStable(options, func(i, j int) bool {
		return options[i].quality > options[j].quality
	})
	if len(options) == 0 || options[0].name == "identity" {
		return ""
	}
	return options[0].name
}

func referenceQuality(t *testing.T, qPattern *regexp.Regexp, headers []string, coding string) float64 {
	t.Helper()
	best := -1.0
	for _, field := range headers {
		for _, item := range strings.Split(field, ",") {
			parts := strings.Split(item, ";")
			if !strings.EqualFold(strings.TrimSpace(parts[0]), coding) {
				continue
			}
			var quality float64
			switch len(parts) {
			case 1:
				quality = 1
			case 2:
				match := qPattern.FindStringSubmatch(parts[1])
				if match != nil {
					var err error
					quality, err = strconv.ParseFloat(match[1], 64)
					if err != nil {
						t.Fatalf("reference could not parse quality %q: %v", match[1], err)
					}
				}
			}
			if best == -1 || quality < best {
				best = quality
			}
		}
	}
	return best
}
