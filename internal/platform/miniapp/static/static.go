package static

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
)

//go:embed all:dist
var embeddedDist embed.FS

const contentSecurityPolicy = "default-src 'self'; " +
	"script-src 'self' https://telegram.org https://challenges.cloudflare.com; " +
	"frame-src https://challenges.cloudflare.com; " +
	"style-src 'self' 'unsafe-inline'; " +
	"connect-src 'self'; " +
	"frame-ancestors https://web.telegram.org"

type Deps struct {
	// FS is rooted at the build output directory. A nil FS uses the embedded build.
	FS fs.FS
}

type Static struct {
	assets map[string]*asset
}

type asset struct {
	raw, br, gzip variant
	contentType   string
	cacheControl  string
}

type variant struct {
	body []byte
	etag string
}

// New snapshots the build output for concurrent serving without filesystem access.
func New(d Deps) (*Static, error) {
	fsys := d.FS
	if fsys == nil {
		var err error
		fsys, err = fs.Sub(embeddedDist, "dist")
		if err != nil {
			return nil, fmt.Errorf("open embedded static build: %w", err)
		}
	}

	s := &Static{assets: make(map[string]*asset)}
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || name == "." {
			return err
		}
		root, _, nested := strings.Cut(name, "/")
		if (root != "v" && root != "s") || strings.HasPrefix(entry.Name(), ".") {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !nested || !entry.Type().IsRegular() {
			return nil
		}
		return s.load(fsys, name)
	})
	if err != nil {
		return nil, fmt.Errorf("load static build: %w", err)
	}
	return s, nil
}

func (s *Static) load(fsys fs.FS, name string) error {
	ext := path.Ext(name)
	var target *variant
	if ext == ".br" || ext == ".gz" {
		// WalkDir visits a file before its suffixed siblings.
		a := s.assets[strings.TrimSuffix(name, ext)]
		if a == nil {
			return nil
		}
		target = &a.gzip
		if ext == ".br" {
			target = &a.br
		}
	} else {
		ext = strings.ToLower(ext)
		a := &asset{contentType: contentType(ext), cacheControl: "no-cache"}
		if ext != ".html" && strings.Contains(name, "/assets/") {
			a.cacheControl = "public, max-age=31536000, immutable"
		}
		s.assets[name] = a
		target = &a.raw
	}

	body, err := fs.ReadFile(fsys, name)
	if err != nil {
		return fmt.Errorf("read static file %q: %w", name, err)
	}
	hash := sha256.Sum256(body)
	*target = variant{body: body, etag: `"` + hex.EncodeToString(hash[:16]) + `"`}
	return nil
}

// Register must be called before the HTTP server starts.
func (s *Static) Register(h interface {
	Handle(pattern string, h http.Handler)
}) {
	h.Handle("GET /v/", http.HandlerFunc(s.serveHTTP))
	h.Handle("GET /s/", http.HandlerFunc(s.serveHTTP))
}

func (s *Static) serveHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Set("Vary", "Accept-Encoding")
	header.Set("Content-Security-Policy", contentSecurityPolicy)
	header.Set("X-Content-Type-Options", "nosniff")

	a := s.lookup(r.URL.Path)
	if a == nil {
		header.Set("Cache-Control", "no-cache")
		header.Set("Content-Type", "text/plain; charset=utf-8")
		writeResponse(w, r, http.StatusNotFound, []byte("404 page not found\n"))
		return
	}

	v := a.raw
	encoding := selectEncoding(r.Header.Values("Accept-Encoding"), a.br.etag != "", a.gzip.etag != "")
	switch encoding {
	case "br":
		v = a.br
	case "gzip":
		v = a.gzip
	}
	if encoding != "" {
		header.Set("Content-Encoding", encoding)
	}
	header.Set("Content-Type", a.contentType)
	header.Set("Cache-Control", a.cacheControl)
	header.Set("ETag", v.etag)
	if matchesETag(r.Header.Values("If-None-Match"), v.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeResponse(w, r, http.StatusOK, v.body)
}

func (s *Static) lookup(urlPath string) *asset {
	if strings.Contains(urlPath, "..") || strings.ContainsAny(urlPath, "\\\x00") {
		return nil
	}
	name, absolute := strings.CutPrefix(urlPath, "/")
	root, _, nested := strings.Cut(name, "/")
	if !absolute || !nested || (root != "v" && root != "s") {
		return nil
	}
	if a := s.assets[name]; a != nil {
		return a
	}
	if strings.Contains(name+"/", "/assets/") || strings.Contains(path.Base(name), ".") {
		return nil
	}
	return s.assets[root+"/index.html"]
}

func writeResponse(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		// The HTTP server's response writer counts write errors.
		w.Write(body)
	}
}

func matchesETag(values []string, etag string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "*" {
			return true
		}
		for {
			value = strings.TrimLeft(value, " \t,")
			value = strings.TrimPrefix(value, "W/")
			if !strings.HasPrefix(value, `"`) {
				break
			}
			end := strings.IndexByte(value[1:], '"')
			if end < 0 {
				break
			}
			tag := value[:end+2]
			value = strings.TrimLeft(value[end+2:], " \t")
			if value != "" && value[0] != ',' {
				break
			}
			if tag == etag {
				return true
			}
		}
	}
	return false
}

func contentType(ext string) string {
	switch ext {
	case ".html":
		return "text/html; charset=utf-8"
	case ".js", ".mjs":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".json", ".map":
		return "application/json"
	case ".svg":
		return "image/svg+xml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".ico":
		return "image/x-icon"
	case ".woff2":
		return "font/woff2"
	case ".txt":
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}
