package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/kc1wfotfm4/qry-backfill-dedup/internal/store"
)

type backfillRequest struct {
	DatasetID      string `json:"dataset_id"`
	BatchID        string `json:"batch_id"`
	IdempotencyKey string `json:"idempotency_key"`
	StartTime      string `json:"start_time"`
	EndTime        string `json:"end_time"`
}

type backfillResponse struct {
	ID             string `json:"id"`
	DatasetID      string `json:"dataset_id"`
	BatchID        string `json:"batch_id"`
	IdempotencyKey string `json:"idempotency_key"`
	StartTime      string `json:"start_time"`
	EndTime        string `json:"end_time"`
	Status         string `json:"status"`
	Progress       int    `json:"progress"`
}

type coverageResponse struct {
	DatasetID string             `json:"datasetId"`
	Intervals []coverageInterval `json:"intervals"`
}

type coverageInterval struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

func createBackfill(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			respondError(c, http.StatusBadRequest, "invalid_request", "request body could not be read")
			return
		}

		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
			respondError(c, http.StatusBadRequest, "invalid_request", "request body must be a JSON object")
			return
		}

		var req backfillRequest

		values := []struct {
			name string
			raw  json.RawMessage
			dst  *string
		}{
			{"dataset_id", fields["dataset_id"], &req.DatasetID},
			{"batch_id", fields["batch_id"], &req.BatchID},
			{"idempotency_key", fields["idempotency_key"], &req.IdempotencyKey},
			{"start_time", fields["start_time"], &req.StartTime},
			{"end_time", fields["end_time"], &req.EndTime},
		}
		for _, field := range values {
			if !decodeStringField(field.raw, field.dst) {
				respondError(c, http.StatusBadRequest, "missing_field", "field "+field.name+" is required and must be a string")
				return
			}
			if *field.dst == "" {
				respondError(c, http.StatusBadRequest, "missing_field", "field "+field.name+" must not be empty")
				return
			}
		}

		start, err := time.Parse(time.RFC3339, req.StartTime)
		if err != nil {
			respondError(c, http.StatusBadRequest, "invalid_time", "start_time must be an RFC 3339 timestamp")
			return
		}
		end, err := time.Parse(time.RFC3339, req.EndTime)
		if err != nil {
			respondError(c, http.StatusBadRequest, "invalid_time", "end_time must be an RFC 3339 timestamp")
			return
		}
		if end.Before(start) {
			respondError(c, http.StatusBadRequest, "inverted_range", "end_time must not be earlier than start_time")
			return
		}

		batch, created, err := st.CreateBackfill(c.Request.Context(), store.Backfill{
			BatchID:        req.BatchID,
			DatasetID:      req.DatasetID,
			IdempotencyKey: req.IdempotencyKey,
			StartTime:      req.StartTime,
			EndTime:        req.EndTime,
		})
		if err != nil {
			if errors.Is(err, store.ErrIdempotencyConflict) {
				respondError(c, http.StatusConflict, "idempotency_conflict", "idempotency_key was already used with different parameters")
				return
			}
			respondError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
			return
		}

		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		c.JSON(status, toBackfillResponse(batch))
	}
}

func getBackfill(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		batch, err := st.GetBackfill(c.Request.Context(), c.Param("id"))
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				respondError(c, http.StatusNotFound, "batch_not_found", "no backfill batch exists with this id")
				return
			}
			respondError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
			return
		}
		c.JSON(http.StatusOK, toBackfillResponse(batch))
	}
}

func getCoverage(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		datasetID := c.Param("dataset_id")
		intervals, err := st.Coverage(c.Request.Context(), datasetID)
		if err != nil {
			respondError(c, http.StatusInternalServerError, "internal_error", "the request could not be completed")
			return
		}
		resp := coverageResponse{DatasetID: datasetID, Intervals: []coverageInterval{}}
		for _, in := range intervals {
			resp.Intervals = append(resp.Intervals, coverageInterval{StartTime: in.StartTime, EndTime: in.EndTime})
		}
		c.JSON(http.StatusOK, resp)
	}
}

func decodeStringField(raw json.RawMessage, dst *string) bool {
	if len(raw) == 0 {
		return false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	*dst = value
	return true
}

func toBackfillResponse(b *store.Backfill) backfillResponse {
	return backfillResponse{
		ID:             b.BatchID,
		DatasetID:      b.DatasetID,
		BatchID:        b.BatchID,
		IdempotencyKey: b.IdempotencyKey,
		StartTime:      b.StartTime,
		EndTime:        b.EndTime,
		Status:         b.Status,
		Progress:       b.Progress,
	}
}

func respondError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
