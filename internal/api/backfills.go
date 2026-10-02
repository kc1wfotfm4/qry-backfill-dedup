package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-backfill-dedup/internal/store"
)

// batchPayload is the public JSON view of one backfill batch.
type batchPayload struct {
	DatasetID      string `json:"dataset_id"`
	BatchID        string `json:"batch_id"`
	IdempotencyKey string `json:"idempotency_key"`
	StartTime      string `json:"start_time"`
	EndTime        string `json:"end_time"`
	ID             string `json:"id"`
	Status         string `json:"status"`
	Progress       int    `json:"progress"`
}

func newBatchPayload(batch store.Batch) batchPayload {
	return batchPayload{
		DatasetID:      batch.DatasetID,
		BatchID:        batch.BatchID,
		IdempotencyKey: batch.IdempotencyKey,
		StartTime:      batch.StartTime,
		EndTime:        batch.EndTime,
		ID:             batch.BatchID,
		Status:         batch.Status,
		Progress:       batch.Progress,
	}
}

type createBatchRequest struct {
	DatasetID      *string `json:"dataset_id"`
	BatchID        *string `json:"batch_id"`
	IdempotencyKey *string `json:"idempotency_key"`
	StartTime      *string `json:"start_time"`
	EndTime        *string `json:"end_time"`
}

// missingField names the first required field absent from the request, or "" when all are set.
func (r *createBatchRequest) missingField() string {
	fields := []struct {
		name  string
		value *string
	}{
		{"dataset_id", r.DatasetID},
		{"batch_id", r.BatchID},
		{"idempotency_key", r.IdempotencyKey},
		{"start_time", r.StartTime},
		{"end_time", r.EndTime},
	}
	for _, field := range fields {
		if field.value == nil {
			return field.name
		}
	}
	return ""
}

func createBatchHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request createBatchRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			writeError(c, http.StatusBadRequest, "invalid_request", "request body must be a valid JSON object")
			return
		}
		if field := request.missingField(); field != "" {
			writeError(c, http.StatusBadRequest, "missing_field", "required field "+field+" is missing")
			return
		}
		startTime, err := time.Parse(time.RFC3339, *request.StartTime)
		if err != nil {
			writeError(c, http.StatusBadRequest, "invalid_time", "start_time must be a valid RFC 3339 timestamp")
			return
		}
		endTime, err := time.Parse(time.RFC3339, *request.EndTime)
		if err != nil {
			writeError(c, http.StatusBadRequest, "invalid_time", "end_time must be a valid RFC 3339 timestamp")
			return
		}
		if endTime.Before(startTime) {
			writeError(c, http.StatusBadRequest, "inverted_range", "end_time must not be earlier than start_time")
			return
		}

		batch := store.Batch{
			DatasetID:      *request.DatasetID,
			BatchID:        *request.BatchID,
			IdempotencyKey: *request.IdempotencyKey,
			StartTime:      *request.StartTime,
			EndTime:        *request.EndTime,
		}
		stored, created, err := st.CreateBatch(c.Request.Context(), batch, startTime.UnixNano(), endTime.UnixNano())
		switch {
		case errors.Is(err, store.ErrIdempotencyConflict):
			writeError(c, http.StatusConflict, "idempotency_conflict", "idempotency_key is already used with different content")
		case err != nil:
			writeError(c, http.StatusInternalServerError, "internal_error", "the service could not record the batch")
		case created:
			c.JSON(http.StatusCreated, newBatchPayload(stored))
		default:
			c.JSON(http.StatusOK, newBatchPayload(stored))
		}
	}
}

func getBatchHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		batch, err := st.GetBatch(c.Request.Context(), c.Param("id"))
		switch {
		case errors.Is(err, store.ErrBatchNotFound):
			writeError(c, http.StatusNotFound, "batch_not_found", "no backfill batch exists with this id")
		case err != nil:
			writeError(c, http.StatusInternalServerError, "internal_error", "the service could not load the batch")
		default:
			c.JSON(http.StatusOK, newBatchPayload(batch))
		}
	}
}

type intervalPayload struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

type coveragePayload struct {
	DatasetID string            `json:"datasetId"`
	Intervals []intervalPayload `json:"intervals"`
}

func getCoverageHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		datasetID := c.Param("dataset_id")
		intervals, err := st.Coverage(c.Request.Context(), datasetID)
		if err != nil {
			writeError(c, http.StatusInternalServerError, "internal_error", "the service could not load the coverage")
			return
		}
		payload := coveragePayload{DatasetID: datasetID, Intervals: make([]intervalPayload, 0, len(intervals))}
		for _, interval := range intervals {
			payload.Intervals = append(payload.Intervals, intervalPayload{
				StartTime: time.Unix(0, interval.StartUnixNs).UTC().Format(time.RFC3339Nano),
				EndTime:   time.Unix(0, interval.EndUnixNs).UTC().Format(time.RFC3339Nano),
			})
		}
		c.JSON(http.StatusOK, payload)
	}
}
