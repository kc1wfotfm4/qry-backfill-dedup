package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-backfill-dedup/internal/store"
)

func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func doRequest(t *testing.T, router *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", recorder.Body.String(), err)
	}
	return body
}

func wantError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, status, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	errorObject, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("body %s has no top-level error object", recorder.Body.String())
	}
	if errorObject["code"] != code {
		t.Fatalf("error code = %v, want %q", errorObject["code"], code)
	}
	if message, ok := errorObject["message"].(string); !ok || message == "" {
		t.Fatalf("error message missing in %s", recorder.Body.String())
	}
}

const validBatchBody = `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`

func postBatch(t *testing.T, router *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	return doRequest(t, router, http.MethodPost, "/api/v1/backfills", body)
}

func TestCreateBatchReturnsCreatedPayload(t *testing.T) {
	router := newTestRouter(t)
	recorder := postBatch(t, router, validBatchBody)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	want := map[string]any{
		"dataset_id":      "ds",
		"batch_id":        "b1",
		"idempotency_key": "k1",
		"start_time":      "2026-01-01T00:00:00Z",
		"end_time":        "2026-01-01T01:00:00Z",
		"id":              "b1",
		"status":          "pending",
		"progress":        float64(0),
	}
	for key, value := range want {
		if body[key] != value {
			t.Fatalf("body[%q] = %v, want %v (body %s)", key, body[key], value, recorder.Body.String())
		}
	}
}

func TestCreateBatchReplaysIdenticalRequest(t *testing.T) {
	router := newTestRouter(t)
	first := postBatch(t, router, validBatchBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want %d", first.Code, http.StatusCreated)
	}

	replay := postBatch(t, router, validBatchBody)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want %d", replay.Code, http.StatusOK)
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}

	coverage := doRequest(t, router, http.MethodGet, "/api/v1/datasets/ds/coverage", "")
	body := decodeBody(t, coverage)
	if intervals := body["intervals"].([]any); len(intervals) != 1 {
		t.Fatalf("coverage = %s, want exactly one interval", coverage.Body.String())
	}
}

func TestCreateBatchRejectsConflictingKey(t *testing.T) {
	router := newTestRouter(t)
	postBatch(t, router, validBatchBody)

	conflict := postBatch(t, router, `{"dataset_id":"ds","batch_id":"b2","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`)
	wantError(t, conflict, http.StatusConflict, "idempotency_conflict")
}

func TestCreateBatchValidation(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"missing dataset_id", `{"batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "missing_field"},
		{"missing batch_id", `{"dataset_id":"ds","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "missing_field"},
		{"missing idempotency_key", `{"dataset_id":"ds","batch_id":"b1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "missing_field"},
		{"missing start_time", `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "missing_field"},
		{"missing end_time", `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z"}`, http.StatusBadRequest, "missing_field"},
		{"bad start_time", `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"soon","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "invalid_time"},
		{"bad end_time", `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01"}`, http.StatusBadRequest, "invalid_time"},
		{"inverted range", `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T02:00:00Z","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "inverted_range"},
		{"malformed json", `{"dataset_id":`, http.StatusBadRequest, "invalid_request"},
		{"empty body", ``, http.StatusBadRequest, "invalid_request"},
		{"wrong field type", `{"dataset_id":42,"batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`, http.StatusBadRequest, "invalid_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(t)
			wantError(t, postBatch(t, router, tc.body), tc.status, tc.code)
		})
	}
}

func TestCreateBatchAllowsZeroLengthRange(t *testing.T) {
	router := newTestRouter(t)
	body := `{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T00:00:00Z"}`
	if recorder := postBatch(t, router, body); recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body %s)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}

	coverage := doRequest(t, router, http.MethodGet, "/api/v1/datasets/ds/coverage", "")
	if got := coverage.Body.String(); !strings.Contains(got, `"intervals":[]`) {
		t.Fatalf("coverage = %s, want empty intervals", got)
	}
}

func TestGetBatchReturnsStoredPayload(t *testing.T) {
	router := newTestRouter(t)
	created := postBatch(t, router, validBatchBody)

	recorder := doRequest(t, router, http.MethodGet, "/api/v1/backfills/b1", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.String() != created.Body.String() {
		t.Fatalf("body = %s, want %s", recorder.Body.String(), created.Body.String())
	}
}

func TestGetBatchReportsMissing(t *testing.T) {
	router := newTestRouter(t)
	wantError(t, doRequest(t, router, http.MethodGet, "/api/v1/backfills/nope", ""), http.StatusNotFound, "batch_not_found")
}

func TestCoverageMergesAndSortsIntervals(t *testing.T) {
	router := newTestRouter(t)
	bodies := []string{
		`{"dataset_id":"ds","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T02:00:00Z"}`,
		`{"dataset_id":"ds","batch_id":"b2","idempotency_key":"k2","start_time":"2026-01-01T01:00:00Z","end_time":"2026-01-01T03:00:00Z"}`,
		`{"dataset_id":"ds","batch_id":"b3","idempotency_key":"k3","start_time":"2026-01-01T03:00:00Z","end_time":"2026-01-01T04:00:00Z"}`,
		`{"dataset_id":"ds","batch_id":"b4","idempotency_key":"k4","start_time":"2026-01-01T06:00:00Z","end_time":"2026-01-01T07:00:00Z"}`,
		`{"dataset_id":"other","batch_id":"b5","idempotency_key":"k5","start_time":"2026-01-01T05:00:00Z","end_time":"2026-01-01T08:00:00Z"}`,
	}
	for _, body := range bodies {
		if recorder := postBatch(t, router, body); recorder.Code != http.StatusCreated {
			t.Fatalf("seed status = %d, want %d (body %s)", recorder.Code, http.StatusCreated, recorder.Body.String())
		}
	}

	recorder := doRequest(t, router, http.MethodGet, "/api/v1/datasets/ds/coverage", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var payload struct {
		DatasetID string `json:"datasetId"`
		Intervals []struct {
			StartTime string `json:"startTime"`
			EndTime   string `json:"endTime"`
		} `json:"intervals"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode coverage: %v", err)
	}
	if payload.DatasetID != "ds" {
		t.Fatalf("datasetId = %q, want %q", payload.DatasetID, "ds")
	}
	want := [][2]string{
		{"2026-01-01T00:00:00Z", "2026-01-01T04:00:00Z"},
		{"2026-01-01T06:00:00Z", "2026-01-01T07:00:00Z"},
	}
	if len(payload.Intervals) != len(want) {
		t.Fatalf("intervals = %+v, want %+v", payload.Intervals, want)
	}
	for i, interval := range want {
		if payload.Intervals[i].StartTime != interval[0] || payload.Intervals[i].EndTime != interval[1] {
			t.Fatalf("intervals[%d] = %+v, want %v", i, payload.Intervals[i], interval)
		}
	}
}

func TestCoverageEmptyDataset(t *testing.T) {
	router := newTestRouter(t)
	recorder := doRequest(t, router, http.MethodGet, "/api/v1/datasets/ghost/coverage", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != `{"datasetId":"ghost","intervals":[]}` {
		t.Fatalf("body = %s", got)
	}
}
