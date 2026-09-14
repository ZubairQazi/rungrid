package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPValidation(t *testing.T) {
	handler := Handler(nil, nil)
	for _, tt := range []struct {
		method, path, body string
		status             int
	}{{"GET", "/healthz", "", 200}, {"GET", "/v1/jobs/not-a-uuid", "", 400}, {"POST", "/v1/jobs", "{", 400}, {"POST", "/v1/jobs", "{\"unknown\":1}", 400}, {"POST", "/v1/jobs/batch", "{}", 400}, {"GET", "/v1/jobs?limit=1001", "", 400}, {"GET", "/v1/jobs?offset=-1", "", 400}, {"GET", "/v1/jobs?limit=abc", "", 400}, {"GET", "/missing", "", 404}, {"DELETE", "/v1/jobs", "", 405}} {
		req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != tt.status {
			t.Errorf("%s %s: %d want %d", tt.method, tt.path, w.Code, tt.status)
		}
	}
}
func TestRejectTrailingJSON(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{} {}"))
	var v map[string]any
	if decode(httptest.NewRecorder(), r, &v) == nil {
		t.Fatal("accepted multiple documents")
	}
}
