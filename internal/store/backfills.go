package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrBatchNotFound reports a lookup that matched no stored batch.
var ErrBatchNotFound = errors.New("batch not found")

// ErrIdempotencyConflict reports an idempotency key reused with different content.
var ErrIdempotencyConflict = errors.New("idempotency key reused with different content")

// Batch is one recorded backfill batch with its persisted progress.
type Batch struct {
	DatasetID      string
	BatchID        string
	IdempotencyKey string
	StartTime      string
	EndTime        string
	Status         string
	Progress       int
}

// contentEquals reports whether two batches carry the same client-supplied content.
func (b Batch) contentEquals(other Batch) bool {
	return b.DatasetID == other.DatasetID &&
		b.BatchID == other.BatchID &&
		b.StartTime == other.StartTime &&
		b.EndTime == other.EndTime
}

// Interval is one merged coverage range; bounds are Unix nanoseconds, start inclusive, end
// exclusive.
type Interval struct {
	StartUnixNs int64
	EndUnixNs   int64
}

// CreateBatch stores a new batch and merges its range into the dataset coverage in one
// transaction. When the idempotency key already exists with identical content it replays the
// stored batch with created=false and leaves coverage untouched; different content under the
// same key fails with ErrIdempotencyConflict.
func (s *Store) CreateBatch(ctx context.Context, batch Batch, startUnixNs, endUnixNs int64) (stored Batch, created bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Batch{}, false, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
INSERT INTO backfill_batches (batch_id, dataset_id, idempotency_key, start_time, end_time, start_unix_ns, end_unix_ns, status, progress)
VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0)`,
		batch.BatchID, batch.DatasetID, batch.IdempotencyKey, batch.StartTime, batch.EndTime, startUnixNs, endUnixNs)
	if err != nil {
		// The idempotency_key unique constraint rejects duplicates; SQLite aborts only the
		// statement, so the stored row can still be read inside this transaction.
		existing, findErr := findBatch(ctx, tx, "idempotency_key = ?", batch.IdempotencyKey)
		if findErr != nil {
			return Batch{}, false, fmt.Errorf("insert batch: %w", err)
		}
		if existing.contentEquals(batch) {
			return existing, false, nil
		}
		return Batch{}, false, ErrIdempotencyConflict
	}

	if endUnixNs > startUnixNs {
		if err := mergeCoverage(ctx, tx, batch.DatasetID, startUnixNs, endUnixNs); err != nil {
			return Batch{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Batch{}, false, fmt.Errorf("commit batch: %w", err)
	}
	batch.Status = "pending"
	batch.Progress = 0
	return batch, true, nil
}

// GetBatch returns the earliest stored batch with the given batch id.
func (s *Store) GetBatch(ctx context.Context, batchID string) (Batch, error) {
	return findBatch(ctx, s.db, "batch_id = ?", batchID)
}

// Coverage lists the merged maximal intervals of one dataset in ascending order.
func (s *Store) Coverage(ctx context.Context, datasetID string) ([]Interval, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT start_unix_ns, end_unix_ns
FROM dataset_coverage
WHERE dataset_id = ?
ORDER BY start_unix_ns ASC`, datasetID)
	if err != nil {
		return nil, fmt.Errorf("query coverage: %w", err)
	}
	defer rows.Close()

	intervals := make([]Interval, 0)
	for rows.Next() {
		var interval Interval
		if err := rows.Scan(&interval.StartUnixNs, &interval.EndUnixNs); err != nil {
			return nil, fmt.Errorf("read coverage: %w", err)
		}
		intervals = append(intervals, interval)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read coverage: %w", err)
	}
	return intervals, nil
}

// querier is satisfied by both *sql.DB and *sql.Tx.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func findBatch(ctx context.Context, q querier, where string, arg string) (Batch, error) {
	row := q.QueryRowContext(ctx, `
SELECT batch_id, dataset_id, idempotency_key, start_time, end_time, status, progress
FROM backfill_batches
WHERE `+where+`
ORDER BY row_id ASC
LIMIT 1`, arg)
	var batch Batch
	err := row.Scan(&batch.BatchID, &batch.DatasetID, &batch.IdempotencyKey, &batch.StartTime, &batch.EndTime, &batch.Status, &batch.Progress)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrBatchNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("query batch: %w", err)
	}
	return batch, nil
}

// mergeCoverage folds [startUnixNs, endUnixNs) into the dataset's stored intervals, absorbing
// every interval that overlaps or touches it, so the table always holds disjoint maximal ranges.
func mergeCoverage(ctx context.Context, tx *sql.Tx, datasetID string, startUnixNs, endUnixNs int64) error {
	rows, err := tx.QueryContext(ctx, `
SELECT start_unix_ns, end_unix_ns
FROM dataset_coverage
WHERE dataset_id = ? AND start_unix_ns <= ? AND end_unix_ns >= ?`,
		datasetID, endUnixNs, startUnixNs)
	if err != nil {
		return fmt.Errorf("find overlapping coverage: %w", err)
	}
	mergedStart, mergedEnd := startUnixNs, endUnixNs
	for rows.Next() {
		var interval Interval
		if err := rows.Scan(&interval.StartUnixNs, &interval.EndUnixNs); err != nil {
			rows.Close()
			return fmt.Errorf("read overlapping coverage: %w", err)
		}
		if interval.StartUnixNs < mergedStart {
			mergedStart = interval.StartUnixNs
		}
		if interval.EndUnixNs > mergedEnd {
			mergedEnd = interval.EndUnixNs
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read overlapping coverage: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("finish overlapping coverage: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
DELETE FROM dataset_coverage
WHERE dataset_id = ? AND start_unix_ns <= ? AND end_unix_ns >= ?`,
		datasetID, endUnixNs, startUnixNs); err != nil {
		return fmt.Errorf("absorb overlapping coverage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO dataset_coverage (dataset_id, start_unix_ns, end_unix_ns)
VALUES (?, ?, ?)`,
		datasetID, mergedStart, mergedEnd); err != nil {
		return fmt.Errorf("store merged coverage: %w", err)
	}
	return nil
}
