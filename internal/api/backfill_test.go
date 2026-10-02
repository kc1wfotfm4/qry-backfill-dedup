package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kc1wfotfm4/qry-backfill-dedup/internal/store"
)

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func doJSON(t *testing.T, router http.Handler, method, target string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, target, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

const validBackfillBody = `{"dataset_id":"ds-1","batch_id":"batch-1","idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return got
}

func expectErrorCode(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, status, rec.Body.String())
	}
	got := decodeBody(t, rec)
	errObj, ok := got["error"].(map[string]any)
	if !ok {
		t.Fatalf("body %s has no top-level error object", rec.Body.String())
	}
	if errObj["code"] != code {
		t.Fatalf("code = %v, want %s; body %s", errObj["code"], code, rec.Body.String())
	}
	if _, ok := errObj["message"].(string); !ok {
		t.Fatalf("message missing in %s", rec.Body.String())
	}
}

func TestCreateBackfillReturnsCreatedBatch(t *testing.T) {
	router := newTestRouter(t)
	rec := doJSON(t, router, http.MethodPost, "/api/v1/backfills", validBackfillBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	got := decodeBody(t, rec)
	want := map[string]any{
		"id":              "batch-1",
		"dataset_id":      "ds-1",
		"batch_id":        "batch-1",
		"idempotency_key": "key-1",
		"start_time":      "2026-01-01T00:00:00Z",
		"end_time":        "2026-01-01T01:00:00Z",
		"status":          "pending",
		"progress":        float64(0),
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s = %v, want %v; body %s", key, got[key], value, rec.Body.String())
		}
	}
}

func TestCreateBackfillRejectsMalformedAndMissingFields(t *testing.T) {
	router := newTestRouter(t)

	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", `{not json`),
		http.StatusBadRequest, "invalid_request")
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", `[]`),
		http.StatusBadRequest, "invalid_request")
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", ``),
		http.StatusBadRequest, "invalid_request")

	missing := `{"dataset_id":"ds-1","idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", missing),
		http.StatusBadRequest, "missing_field")

	wrongType := `{"dataset_id":"ds-1","batch_id":7,"idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", wrongType),
		http.StatusBadRequest, "missing_field")

	emptyValue := `{"dataset_id":"ds-1","batch_id":"","idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", emptyValue),
		http.StatusBadRequest, "missing_field")
}

func TestCreateBackfillValidatesTimesAndRange(t *testing.T) {
	router := newTestRouter(t)

	invalidStart := `{"dataset_id":"ds-1","batch_id":"b1","idempotency_key":"k1","start_time":"yesterday","end_time":"2026-01-01T01:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", invalidStart),
		http.StatusBadRequest, "invalid_time")

	invalidEnd := `{"dataset_id":"ds-1","batch_id":"b2","idempotency_key":"k2","start_time":"2026-01-01T01:00:00Z","end_time":"not-a-time"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", invalidEnd),
		http.StatusBadRequest, "invalid_time")

	inverted := `{"dataset_id":"ds-1","batch_id":"b3","idempotency_key":"k3","start_time":"2026-01-01T02:00:00Z","end_time":"2026-01-01T01:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", inverted),
		http.StatusBadRequest, "inverted_range")

	zeroLength := `{"dataset_id":"ds-1","batch_id":"b4","idempotency_key":"k4","start_time":"2026-01-01T01:00:00Z","end_time":"2026-01-01T01:00:00Z"}`
	if rec := doJSON(t, router, http.MethodPost, "/api/v1/backfills", zeroLength); rec.Code != http.StatusCreated {
		t.Fatalf("zero-length status = %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestCreateBackfillIdempotentReplayAndConflict(t *testing.T) {
	router := newTestRouter(t)

	first := doJSON(t, router, http.MethodPost, "/api/v1/backfills", validBackfillBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body %s", first.Code, first.Body.String())
	}

	replay := doJSON(t, router, http.MethodPost, "/api/v1/backfills", validBackfillBody)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d, body %s", replay.Code, replay.Body.String())
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}

	conflict := `{"dataset_id":"ds-1","batch_id":"batch-1","idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T02:00:00Z"}`
	expectErrorCode(t, doJSON(t, router, http.MethodPost, "/api/v1/backfills", conflict),
		http.StatusConflict, "idempotency_conflict")
}

func TestGetBackfillByID(t *testing.T) {
	router := newTestRouter(t)

	expectErrorCode(t, doJSON(t, router, http.MethodGet, "/api/v1/backfills/unknown", ""),
		http.StatusNotFound, "batch_not_found")

	created := doJSON(t, router, http.MethodPost, "/api/v1/backfills", validBackfillBody)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", created.Code, created.Body.String())
	}

	found := doJSON(t, router, http.MethodGet, "/api/v1/backfills/batch-1", "")
	if found.Code != http.StatusOK {
		t.Fatalf("get status = %d, body %s", found.Code, found.Body.String())
	}
	if found.Body.String() != created.Body.String() {
		t.Fatalf("get body = %s, want %s", found.Body.String(), created.Body.String())
	}
}

func TestCoverageReturnsMergedIntervals(t *testing.T) {
	router := newTestRouter(t)

	empty := doJSON(t, router, http.MethodGet, "/api/v1/datasets/ds-1/coverage", "")
	if empty.Code != http.StatusOK {
		t.Fatalf("empty coverage status = %d", empty.Code)
	}
	if empty.Body.String() != `{"datasetId":"ds-1","intervals":[]}` {
		t.Fatalf("empty coverage body = %s", empty.Body.String())
	}

	batches := []string{
		`{"dataset_id":"ds-1","batch_id":"b1","idempotency_key":"k1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T02:00:00Z"}`,
		`{"dataset_id":"ds-1","batch_id":"b2","idempotency_key":"k2","start_time":"2026-01-01T01:00:00+08:00","end_time":"2026-01-01T03:00:00Z"}`,
		`{"dataset_id":"ds-1","batch_id":"b3","idempotency_key":"k3","start_time":"2026-01-01T03:00:00Z","end_time":"2026-01-01T04:00:00Z"}`,
		`{"dataset_id":"ds-2","batch_id":"b4","idempotency_key":"k4","start_time":"2026-01-02T00:00:00Z","end_time":"2026-01-02T01:00:00Z"}`,
	}
	for _, body := range batches {
		if rec := doJSON(t, router, http.MethodPost, "/api/v1/backfills", body); rec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
		}
	}

	rec := doJSON(t, router, http.MethodGet, "/api/v1/datasets/ds-1/coverage", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("coverage status = %d, body %s", rec.Code, rec.Body.String())
	}
	var got struct {
		DatasetID string `json:"datasetId"`
		Intervals []struct {
			StartTime string `json:"startTime"`
			EndTime   string `json:"endTime"`
		} `json:"intervals"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.DatasetID != "ds-1" || len(got.Intervals) != 1 {
		t.Fatalf("coverage = %s", rec.Body.String())
	}
	start, err := time.Parse(time.RFC3339, got.Intervals[0].StartTime)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	end, err := time.Parse(time.RFC3339, got.Intervals[0].EndTime)
	if err != nil {
		t.Fatalf("parse end: %v", err)
	}
	wantStart := time.Date(2025, 12, 31, 17, 0, 0, 0, time.UTC) // 2026-01-01T01:00:00+08:00
	wantEnd := time.Date(2026, 1, 1, 4, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) || !end.Equal(wantEnd) {
		t.Fatalf("merged interval = %+v", got.Intervals[0])
	}
}
