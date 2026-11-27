package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestErrorForComposesFieldValidationResponse(t *testing.T) {
	a, mux, _, _ := newTestAPI(t)
	a.Mount(testRoutes{{http.MethodPut, "/settings", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.WriteJSON(w, http.StatusUnprocessableEntity, struct {
			Error  ErrorBody                    `json:"error"`
			Fields map[string]map[string]string `json:"fields"`
		}{
			Error:  a.ErrorFor(r, CodeInvalid),
			Fields: map[string]map[string]string{"mode": {"code": "unknown_key"}},
		})
	})}})
	r := makeRequest(http.MethodPut, "/api/v1/settings", validAuthorization(t, 42, "zh"))
	r.Header.Set("Accept-Language", "en")
	w := serve(mux, r)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body = %s", w.Code, w.Body)
	}
	var got struct {
		Error  ErrorBody                    `json:"error"`
		Fields map[string]map[string]string `json:"fields"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body %q: %v", w.Body, err)
	}
	want := ErrorBody{Code: CodeInvalid, Message: "部分字段无效。"}
	if got.Error != want || got.Fields["mode"]["code"] != "unknown_key" {
		t.Errorf("body = %+v, want error %+v beside the field results", got, want)
	}
}

func TestUnauthenticatedErrorUsesFirstAcceptLanguageTag(t *testing.T) {
	_, mux, _, _ := newTestAPI(t)
	for _, tc := range []struct{ header, want string }{
		{"zh,en;q=0.8", "未找到所请求的资源。"},
		{"en,zh;q=0.8", "Not found."},
	} {
		r := makeRequest(http.MethodGet, "/api/v1/missing", "")
		r.Header.Set("Accept-Language", tc.header)
		w := serve(mux, r)
		var got struct{ Error ErrorBody }
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Error.Message != tc.want {
			t.Errorf("Accept-Language %q: message = %q (err %v), want %q", tc.header, got.Error.Message, err, tc.want)
		}
	}
}
