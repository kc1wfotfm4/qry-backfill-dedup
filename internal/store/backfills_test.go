package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func mustCreateBatch(t *testing.T, st *Store, batch Batch, start, end time.Time) Batch {
	t.Helper()
	stored, created, err := st.CreateBatch(context.Background(), batch, start.UnixNano(), end.UnixNano())
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if !created {
		t.Fatalf("create batch: created = false, want true")
	}
	return stored
}

func mustCoverage(t *testing.T, st *Store, datasetID string) []Interval {
	t.Helper()
	intervals, err := st.Coverage(context.Background(), datasetID)
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	return intervals
}

func at(hour int) time.Time {
	return time.Date(2026, 1, 1, hour, 0, 0, 0, time.UTC)
}

func TestCreateBatchPersistsPendingProgress(t *testing.T) {
	st := openTestStore(t)
	batch := Batch{DatasetID: "ds", BatchID: "b1", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T01:00:00Z"}

	stored := mustCreateBatch(t, st, batch, at(0), at(1))
	if stored.Status != "pending" || stored.Progress != 0 {
		t.Fatalf("stored = %+v, want status pending and progress 0", stored)
	}

	loaded, err := st.GetBatch(context.Background(), "b1")
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if loaded != stored {
		t.Fatalf("loaded = %+v, want %+v", loaded, stored)
	}
}

func TestCreateBatchReplaysIdenticalContent(t *testing.T) {
	st := openTestStore(t)
	batch := Batch{DatasetID: "ds", BatchID: "b1", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T01:00:00Z"}
	first := mustCreateBatch(t, st, batch, at(0), at(1))

	replayed, created, err := st.CreateBatch(context.Background(), batch, at(0).UnixNano(), at(1).UnixNano())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if created {
		t.Fatalf("replay: created = true, want false")
	}
	if replayed != first {
		t.Fatalf("replayed = %+v, want %+v", replayed, first)
	}
	if intervals := mustCoverage(t, st, "ds"); len(intervals) != 1 {
		t.Fatalf("coverage = %v, want exactly one interval", intervals)
	}
}

func TestCreateBatchRejectsConflictingKey(t *testing.T) {
	st := openTestStore(t)
	mustCreateBatch(t, st, Batch{DatasetID: "ds", BatchID: "b1", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T01:00:00Z"}, at(0), at(1))

	changed := Batch{DatasetID: "ds", BatchID: "b2", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T01:00:00Z"}
	if _, _, err := st.CreateBatch(context.Background(), changed, at(0).UnixNano(), at(1).UnixNano()); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict err = %v, want ErrIdempotencyConflict", err)
	}
	if _, err := st.GetBatch(context.Background(), "b2"); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("conflicting batch was stored, get err = %v", err)
	}
}

func TestGetBatchReportsMissing(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.GetBatch(context.Background(), "nope"); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("err = %v, want ErrBatchNotFound", err)
	}
}

func TestCoverageMergesOverlappingAndAdjacent(t *testing.T) {
	st := openTestStore(t)
	seed := func(id, key string, start, end time.Time) {
		t.Helper()
		batch := Batch{DatasetID: "ds", BatchID: id, IdempotencyKey: key, StartTime: start.Format(time.RFC3339), EndTime: end.Format(time.RFC3339)}
		mustCreateBatch(t, st, batch, start, end)
	}

	seed("b1", "k1", at(0), at(2))
	seed("b2", "k2", at(1), at(3))
	seed("b3", "k3", at(3), at(4))
	seed("b4", "k4", at(6), at(7))

	intervals := mustCoverage(t, st, "ds")
	want := []Interval{
		{StartUnixNs: at(0).UnixNano(), EndUnixNs: at(4).UnixNano()},
		{StartUnixNs: at(6).UnixNano(), EndUnixNs: at(7).UnixNano()},
	}
	if len(intervals) != len(want) {
		t.Fatalf("coverage = %v, want %v", intervals, want)
	}
	for i := range want {
		if intervals[i] != want[i] {
			t.Fatalf("coverage[%d] = %v, want %v", i, intervals[i], want[i])
		}
	}
}

func TestCoverageIgnoresZeroLengthAndForeignDatasets(t *testing.T) {
	st := openTestStore(t)
	zero := Batch{DatasetID: "ds", BatchID: "b0", IdempotencyKey: "k0", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T00:00:00Z"}
	mustCreateBatch(t, st, zero, at(0), at(0))
	other := Batch{DatasetID: "other", BatchID: "b1", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T01:00:00Z"}
	mustCreateBatch(t, st, other, at(0), at(1))

	if intervals := mustCoverage(t, st, "ds"); len(intervals) != 0 {
		t.Fatalf("coverage = %v, want empty", intervals)
	}
	if intervals := mustCoverage(t, st, "other"); len(intervals) != 1 {
		t.Fatalf("other coverage = %v, want one interval", intervals)
	}
}
