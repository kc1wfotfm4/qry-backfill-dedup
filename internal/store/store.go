// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound reports that no batch matches the lookup key.
var ErrNotFound = errors.New("backfill batch not found")

// ErrIdempotencyConflict reports that an idempotency key is already stored
// together with different request parameters.
var ErrIdempotencyConflict = errors.New("idempotency key was already used with different parameters")

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	query := url.Values{}
	query.Set("_txlock", "immediate")
	dsn := url.URL{Path: path, RawQuery: query.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// Backfill is the persisted record of one backfill batch.
type Backfill struct {
	BatchID        string
	DatasetID      string
	IdempotencyKey string
	StartTime      string
	EndTime        string
	Status         string
	Progress       int
}

// Interval is one merged coverage window.
type Interval struct {
	StartTime string
	EndTime   string
}

// CreateBackfill persists a new batch and folds its range into the dataset
// coverage inside one transaction. Replaying an idempotency key with the same
// parameters returns the stored batch without writing anything. The same key
// paired with different parameters returns ErrIdempotencyConflict.
func (s *Store) CreateBackfill(ctx context.Context, b Backfill) (*Backfill, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin create backfill: %w", err)
	}
	defer tx.Rollback()

	if existing, err := lookupByIdempotencyKey(ctx, tx, b.IdempotencyKey); err != nil {
		return nil, false, err
	} else if existing != nil {
		if sameInput(existing, &b) {
			return existing, false, nil
		}
		return nil, false, ErrIdempotencyConflict
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO backfills (batch_id, dataset_id, idempotency_key, start_time, end_time, status, progress)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		b.BatchID, b.DatasetID, b.IdempotencyKey, b.StartTime, b.EndTime, "pending", 0); err != nil {
		if isUniqueConstraint(err) {
			// A concurrent request won the idempotency key; replay its outcome.
			existing, lookupErr := lookupByIdempotencyKey(ctx, tx, b.IdempotencyKey)
			if lookupErr != nil {
				return nil, false, lookupErr
			}
			if existing != nil {
				if sameInput(existing, &b) {
					return existing, false, nil
				}
				return nil, false, ErrIdempotencyConflict
			}
		}
		return nil, false, fmt.Errorf("insert backfill: %w", err)
	}

	if err := mergeCoverage(ctx, tx, b.DatasetID, b.StartTime, b.EndTime); err != nil {
		return nil, false, err
	}

	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit create backfill: %w", err)
	}
	b.Status = "pending"
	b.Progress = 0
	return &b, true, nil
}

// GetBackfill returns the batch with the given batch identifier.
func (s *Store) GetBackfill(ctx context.Context, batchID string) (*Backfill, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT dataset_id, batch_id, idempotency_key, start_time, end_time, status, progress
		 FROM backfills WHERE batch_id = ? ORDER BY id LIMIT 1`, batchID)
	b, err := scanBackfill(row)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrNotFound
	}
	return b, nil
}

// Coverage returns the merged coverage windows of one dataset in ascending
// time order. Datasets without coverage return an empty slice.
func (s *Store) Coverage(ctx context.Context, datasetID string) ([]Interval, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT start_text, end_text FROM coverage_intervals WHERE dataset_id = ? ORDER BY start_key, end_key DESC`,
		datasetID)
	if err != nil {
		return nil, fmt.Errorf("query coverage: %w", err)
	}
	defer rows.Close()

	intervals := make([]Interval, 0)
	for rows.Next() {
		var in Interval
		if err := rows.Scan(&in.StartTime, &in.EndTime); err != nil {
			return nil, fmt.Errorf("scan coverage: %w", err)
		}
		intervals = append(intervals, in)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coverage: %w", err)
	}
	return intervals, nil
}

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// lookupByIdempotencyKey returns the batch carrying idempotencyKey, or nil
// when the key is unused.
func lookupByIdempotencyKey(ctx context.Context, q rowQuerier, idempotencyKey string) (*Backfill, error) {
	row := q.QueryRowContext(ctx,
		`SELECT dataset_id, batch_id, idempotency_key, start_time, end_time, status, progress
		 FROM backfills WHERE idempotency_key = ?`, idempotencyKey)
	return scanBackfill(row)
}

func scanBackfill(row *sql.Row) (*Backfill, error) {
	b := &Backfill{}
	err := row.Scan(&b.DatasetID, &b.BatchID, &b.IdempotencyKey, &b.StartTime, &b.EndTime, &b.Status, &b.Progress)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load backfill: %w", err)
	}
	return b, nil
}

func sameInput(a, b *Backfill) bool {
	return a.DatasetID == b.DatasetID &&
		a.BatchID == b.BatchID &&
		a.StartTime == b.StartTime &&
		a.EndTime == b.EndTime
}

// mergeCoverage folds a new [startText, endText) window into the stored
// coverage of datasetID, merging overlapping windows and windows that touch
// (previous end equals next start).
func mergeCoverage(ctx context.Context, tx *sql.Tx, datasetID, startText, endText string) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT start_text, end_text, start_key, end_key FROM coverage_intervals WHERE dataset_id = ? ORDER BY start_key, end_key DESC`,
		datasetID)
	if err != nil {
		return fmt.Errorf("load coverage: %w", err)
	}

	type window struct {
		startText, endText string
		start, end         time.Time
	}
	windows := make([]window, 0)
	for rows.Next() {
		var w window
		var startKey, endKey string
		if err := rows.Scan(&w.startText, &w.endText, &startKey, &endKey); err != nil {
			rows.Close()
			return fmt.Errorf("scan coverage: %w", err)
		}
		if w.start, err = time.Parse(time.RFC3339, w.startText); err != nil {
			rows.Close()
			return fmt.Errorf("parse coverage start: %w", err)
		}
		if w.end, err = time.Parse(time.RFC3339, w.endText); err != nil {
			rows.Close()
			return fmt.Errorf("parse coverage end: %w", err)
		}
		windows = append(windows, w)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate coverage: %w", err)
	}
	rows.Close()

	newStart, err := time.Parse(time.RFC3339, startText)
	if err != nil {
		return fmt.Errorf("parse backfill start: %w", err)
	}
	newEnd, err := time.Parse(time.RFC3339, endText)
	if err != nil {
		return fmt.Errorf("parse backfill end: %w", err)
	}
	windows = append(windows, window{startText: startText, endText: endText, start: newStart, end: newEnd})
	sort.Slice(windows, func(i, j int) bool {
		if !windows[i].start.Equal(windows[j].start) {
			return windows[i].start.Before(windows[j].start)
		}
		return windows[i].end.After(windows[j].end)
	})

	merged := make([]window, 0, len(windows))
	for _, w := range windows {
		if len(merged) == 0 || w.start.After(merged[len(merged)-1].end) {
			merged = append(merged, w)
			continue
		}
		if w.end.After(merged[len(merged)-1].end) {
			merged[len(merged)-1].end = w.end
			merged[len(merged)-1].endText = w.endText
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM coverage_intervals WHERE dataset_id = ?`, datasetID); err != nil {
		return fmt.Errorf("reset coverage: %w", err)
	}
	for _, w := range merged {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO coverage_intervals (dataset_id, start_text, end_text, start_key, end_key) VALUES (?, ?, ?, ?, ?)`,
			datasetID, w.startText, w.endText, sortKey(w.start), sortKey(w.end)); err != nil {
			return fmt.Errorf("write coverage: %w", err)
		}
	}
	return nil
}

// sortKey renders a time as a fixed-width UTC string so lexicographic order
// matches chronological order for every RFC 3339 instant the API accepts.
func sortKey(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func isUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfills (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	batch_id        TEXT NOT NULL,
	dataset_id      TEXT NOT NULL,
	idempotency_key TEXT NOT NULL UNIQUE,
	start_time      TEXT NOT NULL,
	end_time        TEXT NOT NULL,
	status          TEXT NOT NULL,
	progress        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_backfills_batch_id ON backfills(batch_id);
CREATE INDEX IF NOT EXISTS idx_backfills_dataset_id ON backfills(dataset_id);

CREATE TABLE IF NOT EXISTS coverage_intervals (
	dataset_id  TEXT NOT NULL,
	start_text  TEXT NOT NULL,
	end_text    TEXT NOT NULL,
	start_key   TEXT NOT NULL,
	end_key     TEXT NOT NULL,
	PRIMARY KEY (dataset_id, start_key, end_key)
);
`
