package static

import (
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

const expectedCSP = "default-src 'self'; script-src 'self' https://telegram.org https://challenges.cloudflare.com; frame-src https://challenges.cloudflare.com; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors https://web.telegram.org"

func TestRepresentationHeaders(t *testing.T) {
	t.Parallel()
	mux := staticMux(t, fstest.MapFS{
		"v/assets/main-a123.js":    {Data: []byte("hello")},
		"v/assets/main-a123.js.br": {Data: []byte("brotli bytes")},
		"v/assets/main-a123.js.gz": {Data: []byte("gzip bytes")},
	})

	for _, tc := range []struct {
		name   string
		accept []string
		coding string
		body   string
	}{
		{"identity", nil, "", "hello"},
		{"brotli", []string{"gzip, br"}, "br", "brotli bytes"},
		{"gzip", []string{"gzip;q=0.5, br;q=0.4"}, "gzip", "gzip bytes"},
		{"separate header fields", []string{"gzip;q=0.1", "br;q=0.8"}, "br", "brotli bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := request(mux, http.MethodGet, "/v/assets/main-a123.js", http.Header{
				"Accept-Encoding": tc.accept,
				"Range":           {"bytes=0-1"},
			})
			assertResponse(t, res, http.StatusOK, tc.body)
			assertHeaders(t, res, map[string]string{
				"Content-Encoding": tc.coding,
				"Content-Type":     "text/javascript; charset=utf-8",
				"Content-Length":   strconv.Itoa(len(tc.body)),
				"Content-Range":    "",
				"Last-Modified":    "",
			})
			assertStaticHeaders(t, res)
		})
	}
}

func TestContentTypes(t *testing.T) {
	t.Parallel()
	cases := []struct{ extension, want string }{
		{"html", "text/html; charset=utf-8"},
		{"js", "text/javascript; charset=utf-8"},
		{"mjs", "text/javascript; charset=utf-8"},
		{"css", "text/css; charset=utf-8"},
		{"json", "application/json"},
		{"svg", "image/svg+xml"},
		{"png", "image/png"},
		{"jpg", "image/jpeg"},
		{"jpeg", "image/jpeg"},
		{"webp", "image/webp"},
		{"ico", "image/x-icon"},
		{"woff2", "font/woff2"},
		{"txt", "text/plain; charset=utf-8"},
		{"map", "application/json"},
		{"JS", "text/javascript; charset=utf-8"},
		{"unknown", "application/octet-stream"},
	}
	files := fstest.MapFS{}
	for _, tc := range cases {
		files["s/file."+tc.extension] = &fstest.MapFile{Data: []byte("content")}
	}
	mux := staticMux(t, files)
	for _, tc := range cases {
		t.Run(tc.extension, func(t *testing.T) {
			res := request(mux, http.MethodGet, "/s/file."+tc.extension, nil)
			assertResponse(t, res, http.StatusOK, "content")
			assertHeaders(t, res, map[string]string{"Content-Type": tc.want})
		})
	}
}

func TestCacheControl(t *testing.T) {
	t.Parallel()
	mux := staticMux(t, fstest.MapFS{
		"v/index.html":           {Data: []byte("entry")},
		"v/assets/x-abc123.js":   {Data: []byte("hashed")},
		"s/sub/assets/plain.css": {Data: []byte("nested")},
		"v/assets/info.html":     {Data: []byte("asset html")},
		"v/not-assets/file.js":   {Data: []byte("other directory")},
		"v/plain-name.txt":       {Data: []byte("plain")},
	})

	for _, tc := range []struct{ path, body, cache string }{
		{"/v/assets/x-abc123.js", "hashed", "public, max-age=31536000, immutable"},
		{"/s/sub/assets/plain.css", "nested", "public, max-age=31536000, immutable"},
		{"/v/assets/info.html", "asset html", "no-cache"},
		{"/v/index.html", "entry", "no-cache"},
		{"/v/", "entry", "no-cache"},
		{"/v/not-assets/file.js", "other directory", "no-cache"},
		{"/v/plain-name.txt", "plain", "no-cache"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			res := request(mux, http.MethodGet, tc.path, nil)
			assertResponse(t, res, http.StatusOK, tc.body)
			assertHeaders(t, res, map[string]string{"Cache-Control": tc.cache})
		})
	}
}

func TestRepresentationETags(t *testing.T) {
	t.Parallel()
	const target = "/v/assets/main-a123.js"
	files := fstest.MapFS{
		"v/assets/main-a123.js":    {Data: []byte("hello")},
		"v/assets/main-a123.js.br": {Data: []byte("br data")},
		"v/assets/main-a123.js.gz": {Data: []byte("gz data")},
		"v/copy.js":                {Data: []byte("hello")},
	}
	mux := staticMux(t, files)
	raw := request(mux, http.MethodGet, target, nil).Header().Get("ETag")
	br := request(mux, http.MethodGet, target, http.Header{"Accept-Encoding": {"br"}}).Header().Get("ETag")
	gzip := request(mux, http.MethodGet, target, http.Header{"Accept-Encoding": {"gzip"}}).Header().Get("ETag")
	tags := map[string]string{
		"identity": raw,
		"br":       br,
		"gzip":     gzip,
	}
	for encoding, tag := range tags {
		for other, otherTag := range tags {
			if encoding != other && tag == otherTag {
				t.Errorf("%s and %s share ETag %q", encoding, other, tag)
			}
		}
	}
	if raw != "\"2cf24dba5fb0a30e26e83b2ac5b9e29e\"" {
		t.Errorf("raw ETag = %q, want the quoted first 16 SHA-256 bytes of hello", raw)
	}
	if duplicate := request(mux, http.MethodGet, "/v/copy.js", nil).Header().Get("ETag"); duplicate != raw {
		t.Errorf("equal bytes have ETags %q and %q", raw, duplicate)
	}

	for _, tc := range []struct {
		name      string
		matchTags []string
		status    int
		body      string
	}{
		{"strong match", []string{br}, http.StatusNotModified, ""},
		{"weak match", []string{"W/" + br}, http.StatusNotModified, ""},
		{"list with quoted comma", []string{"\"other,tag\", W/" + br}, http.StatusNotModified, ""},
		{"separate header fields", []string{raw, br}, http.StatusNotModified, ""},
		{"wildcard", []string{" * "}, http.StatusNotModified, ""},
		{"different variants", []string{raw + ", W/" + gzip}, http.StatusOK, "br data"},
		{"different tag", []string{"\"other\""}, http.StatusOK, "br data"},
		{"malformed tag", []string{br + "suffix"}, http.StatusOK, "br data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := request(mux, http.MethodGet, target, http.Header{
				"Accept-Encoding": {"br"},
				"If-None-Match":   tc.matchTags,
			})
			assertResponse(t, res, tc.status, tc.body)
			assertHeaders(t, res, map[string]string{
				"Cache-Control":    "public, max-age=31536000, immutable",
				"Content-Encoding": "br",
				"ETag":             br,
			})
			assertStaticHeaders(t, res)
		})
	}

	files["v/assets/main-a123.js"] = &fstest.MapFile{Data: []byte("changed")}
	updated := staticMux(t, files)
	res := request(updated, http.MethodGet, target, http.Header{"If-None-Match": {raw}})
	assertResponse(t, res, http.StatusOK, "changed")
	if tag := res.Header().Get("ETag"); tag == "" || tag == raw {
		t.Errorf("changed bytes have ETag %q, previous ETag %q", tag, raw)
	}
}

func TestSPAFallback(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"v/index.html":     {Data: []byte("verification")},
		"v/index.html.br":  {Data: []byte("compressed verification")},
		"v/health":         {Data: []byte("exact file")},
		"s/index.html":     {Data: []byte("settings")},
		"s/assets/real.js": {Data: []byte("settings script")},
	}
	mux := staticMux(t, files)
	noEntries := staticMux(t, fstest.MapFS{})

	for _, tc := range []struct {
		path, body string
		status     int
		noIndex    bool
	}{
		{"/v/", "verification", http.StatusOK, false},
		{"/v/settings/123", "verification", http.StatusOK, false},
		{"/v/health", "exact file", http.StatusOK, false},
		{"/v/missing.js", "404 page not found\n", http.StatusNotFound, false},
		{"/v/assets/missing", "404 page not found\n", http.StatusNotFound, false},
		{"/v/assets/", "404 page not found\n", http.StatusNotFound, false},
		{"/s/", "settings", http.StatusOK, false},
		{"/s/chats/42", "settings", http.StatusOK, false},
		{"/s/missing.css", "404 page not found\n", http.StatusNotFound, false},
		{"/s/assets/missing", "404 page not found\n", http.StatusNotFound, false},
		{"/v/", "404 page not found\n", http.StatusNotFound, true},
		{"/s/chats/42", "404 page not found\n", http.StatusNotFound, true},
	} {
		t.Run(tc.path, func(t *testing.T) {
			handler := mux
			if tc.noIndex {
				handler = noEntries
			}
			res := request(handler, http.MethodGet, tc.path, nil)
			assertResponse(t, res, tc.status, tc.body)
			assertStaticHeaders(t, res)
		})
	}

	res := request(mux, http.MethodGet, "/v/settings/123", http.Header{"Accept-Encoding": {"br"}})
	assertResponse(t, res, http.StatusOK, "compressed verification")
	assertHeaders(t, res, map[string]string{
		"Content-Encoding": "br",
		"Content-Type":     "text/html; charset=utf-8",
		"Cache-Control":    "no-cache",
	})
}

func TestHeadMatchesGetHeaders(t *testing.T) {
	t.Parallel()
	mux := staticMux(t, fstest.MapFS{
		"v/index.html":    {Data: []byte("verification")},
		"v/index.html.br": {Data: []byte("br verification")},
	})
	for _, tc := range []struct {
		name, path string
		fields     http.Header
		status     int
	}{
		{"compressed", "/v/", http.Header{"Accept-Encoding": {"br"}}, http.StatusOK},
		{"not modified", "/v/", http.Header{"If-None-Match": {"*"}}, http.StatusNotModified},
		{"not found", "/v/missing.js", nil, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			head := request(mux, http.MethodHead, tc.path, tc.fields)
			get := request(mux, http.MethodGet, tc.path, tc.fields)
			assertResponse(t, head, tc.status, "")
			if get.Code != tc.status || !reflect.DeepEqual(head.Result().Header, get.Result().Header) {
				t.Errorf("HEAD headers %v differ from GET (%d) headers %v", head.Header(), get.Code, get.Header())
			}
		})
	}
}

func TestPathsStayWithinEntryRoots(t *testing.T) {
	t.Parallel()
	mux := staticMux(t, fstest.MapFS{
		"secret.txt":      {Data: []byte("root secret")},
		"v/index.html":    {Data: []byte("verification")},
		"s/index.html":    {Data: []byte("settings")},
		"s/settings.json": {Data: []byte("settings data")},
	})
	direct, _ := mux.Handler(httptest.NewRequest(http.MethodGet, "/v/", nil))
	for _, tc := range []struct {
		target     string
		muxStatus  int
		redirectTo string
	}{
		{"/secret.txt", http.StatusNotFound, ""},
		{"/v/../secret.txt", http.StatusTemporaryRedirect, "/secret.txt"},
		{"/s/../secret.txt", http.StatusTemporaryRedirect, "/secret.txt"},
		{"/v/%2e%2e/secret.txt", http.StatusNotFound, ""},
		{"/v/%2e%2E%2fsecret.txt", http.StatusNotFound, ""},
		{"/v/..%2fs/settings.json", http.StatusNotFound, ""},
		{"/s/%5c..%5csecret.txt", http.StatusNotFound, ""},
		{"/v/invalid%5cname", http.StatusNotFound, ""},
		{"/v/invalid%00name", http.StatusNotFound, ""},
		{"/v/settings..old", http.StatusNotFound, ""},
	} {
		t.Run(tc.target, func(t *testing.T) {
			assertResponse(t, request(direct, http.MethodGet, tc.target, nil), http.StatusNotFound, "404 page not found\n")
			res := request(mux, http.MethodGet, tc.target, nil)
			if tc.redirectTo == "" {
				assertResponse(t, res, tc.muxStatus, "404 page not found\n")
				return
			}
			if res.Code != tc.muxStatus || res.Header().Get("Location") != tc.redirectTo {
				t.Fatalf("mux returned %d, Location %q", res.Code, res.Header().Get("Location"))
			}
			assertResponse(t, request(mux, http.MethodGet, tc.redirectTo, nil), http.StatusNotFound, "404 page not found\n")
		})
	}
}

func TestLoadSnapshotsOnlyServedFiles(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"v/.dot.js":          {Data: []byte("hidden file")},
		"v/.hidden/in.txt":   {Data: []byte("hidden directory")},
		"v/_chunk.js":        {Data: []byte("chunk")},
		"v/_chunk.js.gz":     {Data: []byte("gz chunk")},
		"v/_chunk.js.gz.br":  {Data: []byte("orphan nested variant")},
		"v/standalone.js.br": {Data: []byte("orphan br")},
		"s/standalone.js.gz": {Data: []byte("orphan gz")},
		"v/empty.txt":        {},
	}
	for _, ignored := range []string{"v/.dot.js", "v/.hidden", "v/standalone.js.br", "s/standalone.js.gz"} {
		t.Run("unopened "+ignored, func(t *testing.T) {
			staticMux(t, unreadableFS{FS: files, name: ignored})
		})
	}

	mux := staticMux(t, files)
	files["v/_chunk.js"].Data[0] = 'X'
	clear(files)
	for _, tc := range []struct {
		path, accept, encoding, body string
		status                       int
	}{
		{"/v/_chunk.js", "", "", "chunk", http.StatusOK},
		{"/v/_chunk.js", "br, gzip", "gzip", "gz chunk", http.StatusOK},
		{"/v/_chunk.js", "br", "", "chunk", http.StatusOK},
		{"/v/empty.txt", "gzip", "", "", http.StatusOK},
		{"/v/.dot.js", "", "", "404 page not found\n", http.StatusNotFound},
		{"/v/.hidden/in.txt", "", "", "404 page not found\n", http.StatusNotFound},
		{"/v/_chunk.js.gz", "", "", "404 page not found\n", http.StatusNotFound},
		{"/v/standalone.js.br", "", "", "404 page not found\n", http.StatusNotFound},
		{"/s/standalone.js.gz", "", "", "404 page not found\n", http.StatusNotFound},
	} {
		t.Run(tc.path+" "+tc.accept, func(t *testing.T) {
			res := request(mux, http.MethodGet, tc.path, http.Header{"Accept-Encoding": {tc.accept}})
			assertResponse(t, res, tc.status, tc.body)
			assertHeaders(t, res, map[string]string{"Content-Encoding": tc.encoding})
		})
	}
}

func TestLoadReturnsFilesystemErrors(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{"v/index.html": {}, "v/index.html.br": {}}
	for _, name := range []string{".", "v", "v/index.html", "v/index.html.br"} {
		t.Run(name, func(t *testing.T) {
			s, err := New(Deps{FS: unreadableFS{FS: files, name: name}})
			if s != nil || !errors.Is(err, fs.ErrPermission) || !strings.Contains(err.Error(), name) {
				t.Fatalf("New returned (%v, %v), want a permission error for %q", s, err, name)
			}
		})
	}
}

func TestEmbeddedBuildLoads(t *testing.T) {
	t.Parallel()
	if _, err := New(Deps{}); err != nil {
		t.Fatal(err)
	}
}

type unreadableFS struct {
	fs.FS
	name string
}

func (f unreadableFS) Open(name string) (fs.File, error) {
	if name == f.name {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return f.FS.Open(name)
}

func staticMux(t *testing.T, files fs.FS) *http.ServeMux {
	t.Helper()
	s, err := New(Deps{FS: files})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.Register(mux)
	return mux
}

func request(handler http.Handler, method, path string, fields http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header = fields
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

func assertResponse(t *testing.T, res *httptest.ResponseRecorder, status int, body string) {
	t.Helper()
	if res.Code != status || res.Body.String() != body {
		t.Fatalf("got status %d and body %q, want status %d and body %q", res.Code, res.Body.String(), status, body)
	}
}

func assertHeaders(t *testing.T, res *httptest.ResponseRecorder, expected map[string]string) {
	t.Helper()
	for name, want := range expected {
		if got := res.Result().Header.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func assertStaticHeaders(t *testing.T, res *httptest.ResponseRecorder) {
	t.Helper()
	assertHeaders(t, res, map[string]string{
		"Vary":                    "Accept-Encoding",
		"X-Content-Type-Options":  "nosniff",
		"Content-Security-Policy": expectedCSP,
		"X-Frame-Options":         "",
	})
}
