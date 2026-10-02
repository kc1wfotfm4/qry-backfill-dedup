package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
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

func TestOpenCreatesUsableStore(t *testing.T) {
	st := openTestStore(t)
	if err := st.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func sampleBackfill() Backfill {
	return Backfill{
		BatchID:        "batch-1",
		DatasetID:      "dataset-1",
		IdempotencyKey: "key-1",
		StartTime:      "2026-01-01T00:00:00Z",
		EndTime:        "2026-01-01T01:00:00Z",
	}
}

func TestCreateBackfillPersistsPendingBatch(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	batch, created, err := st.CreateBackfill(ctx, sampleBackfill())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created {
		t.Fatalf("created = false, want true")
	}
	if batch.Status != "pending" || batch.Progress != 0 || batch.BatchID != "batch-1" {
		t.Fatalf("unexpected batch: %+v", batch)
	}

	got, err := st.GetBackfill(ctx, "batch-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.IdempotencyKey != "key-1" || got.StartTime != batch.StartTime || got.EndTime != batch.EndTime {
		t.Fatalf("stored batch mismatch: %+v", got)
	}
}

func TestCreateBackfillAllowsZeroLengthRange(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	b := sampleBackfill()
	b.IdempotencyKey = "key-zero"
	b.EndTime = b.StartTime

	if _, created, err := st.CreateBackfill(ctx, b); err != nil || !created {
		t.Fatalf("zero-length create: created=%v err=%v", created, err)
	}
}

func TestReplaySameKeyReturnsStoredBatchWithoutWrite(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	first, created, err := st.CreateBackfill(ctx, sampleBackfill())
	if err != nil || !created {
		t.Fatalf("first create: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE backfills SET progress = 42 WHERE idempotency_key = ?`, first.IdempotencyKey); err != nil {
		t.Fatalf("update progress: %v", err)
	}

	replay, created, err := st.CreateBackfill(ctx, sampleBackfill())
	if err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if replay.Progress != 42 {
		t.Fatalf("progress = %d, want persisted 42 (replay must not reset)", replay.Progress)
	}

	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfills`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("backfill count = %d, want 1", count)
	}
}

func TestReplaySameKeyDifferentContentConflicts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	b := sampleBackfill()
	if _, _, err := st.CreateBackfill(ctx, b); err != nil {
		t.Fatalf("create: %v", err)
	}
	b.EndTime = "2026-01-01T02:00:00Z"
	_, _, err := st.CreateBackfill(ctx, b)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestGetBackfillUnknownReturnsNotFound(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.GetBackfill(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestCoverageMergesOverlappingAndTouchingWindows(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	cases := []Backfill{
		{BatchID: "b1", DatasetID: "d", IdempotencyKey: "k1", StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T02:00:00Z"},
		{BatchID: "b2", DatasetID: "d", IdempotencyKey: "k2", StartTime: "2026-01-01T01:00:00Z", EndTime: "2026-01-01T03:00:00Z"},
		{BatchID: "b3", DatasetID: "d", IdempotencyKey: "k3", StartTime: "2026-01-01T03:00:00Z", EndTime: "2026-01-01T04:00:00Z"},
		{BatchID: "b4", DatasetID: "d", IdempotencyKey: "k4", StartTime: "2026-01-01T05:00:00Z", EndTime: "2026-01-01T06:00:00Z"},
	}
	for _, b := range cases {
		if _, _, err := st.CreateBackfill(ctx, b); err != nil {
			t.Fatalf("create %s: %v", b.BatchID, err)
		}
	}

	intervals, err := st.Coverage(ctx, "d")
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	want := []Interval{
		{StartTime: "2026-01-01T00:00:00Z", EndTime: "2026-01-01T04:00:00Z"},
		{StartTime: "2026-01-01T05:00:00Z", EndTime: "2026-01-01T06:00:00Z"},
	}
	if len(intervals) != len(want) {
		t.Fatalf("intervals = %+v, want %+v", intervals, want)
	}
	for i := range want {
		if intervals[i] != want[i] {
			t.Fatalf("interval %d = %+v, want %+v", i, intervals[i], want[i])
		}
	}

	other, err := st.Coverage(ctx, "other")
	if err != nil || len(other) != 0 {
		t.Fatalf("other coverage = %+v, %v", other, err)
	}
}

func TestConcurrentCreatesWithSameKeyDeduplicate(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	const workers = 12
	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, _, err := st.CreateBackfill(ctx, sampleBackfill())
			if err != nil {
				if !errors.Is(err, ErrIdempotencyConflict) {
					t.Errorf("create: %v", err)
					return
				}
				mu.Lock()
				conflicts++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfills`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("backfill count = %d, want 1 (conflicts=%d)", count, conflicts)
	}
}

func TestCreateRollsBackBatchWhenCoverageMergeFails(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)

	bad := sampleBackfill()
	bad.IdempotencyKey = "key-bad"
	bad.EndTime = "not-a-timestamp"
	if _, _, err := st.CreateBackfill(ctx, bad); err == nil {
		t.Fatalf("create with bad time: want error")
	}

	var batches, intervals int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfills`).Scan(&batches); err != nil {
		t.Fatalf("count backfills: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coverage_intervals`).Scan(&intervals); err != nil {
		t.Fatalf("count intervals: %v", err)
	}
	if batches != 0 || intervals != 0 {
		t.Fatalf("batches=%d intervals=%d, want 0/0 after rollback", batches, intervals)
	}
}
