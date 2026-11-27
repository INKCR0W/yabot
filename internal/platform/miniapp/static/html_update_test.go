package static

import (
	"net/http"
	"testing"
	"testing/fstest"
)

func TestUpdatedHTMLIsNotRevalidatedWithOldETag(t *testing.T) {
	t.Parallel()
	build := func(html string) http.Handler {
		return staticMux(t, fstest.MapFS{
			"v/index.html":    {Data: []byte(html)},
			"v/index.html.gz": {Data: []byte(html + " gz")},
		})
	}
	before, after := build("old"), build("new")
	for _, tc := range []struct {
		name, path string
		header     http.Header
		wantBody   string
	}{
		{"entry file", "/v/index.html", http.Header{}, "new"},
		{"fallback route gzip", "/v/chats/42", http.Header{"Accept-Encoding": {"gzip"}}, "new gz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := request(before, http.MethodGet, tc.path, tc.header).Header().Get("ETag")
			header := tc.header.Clone()
			header.Set("If-None-Match", old)
			res := request(after, http.MethodGet, tc.path, header)
			assertResponse(t, res, http.StatusOK, tc.wantBody)
			if tag := res.Header().Get("ETag"); tag == "" || tag == old {
				t.Errorf("updated HTML has ETag %q, previous ETag %q", tag, old)
			}
		})
	}
}
