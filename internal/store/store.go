// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection serializes writers so the batch insert and the coverage merge stay
	// one atomic unit even under concurrent requests.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
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

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill_batches (
	row_id          INTEGER PRIMARY KEY AUTOINCREMENT,
	batch_id        TEXT NOT NULL,
	dataset_id      TEXT NOT NULL,
	idempotency_key TEXT NOT NULL UNIQUE,
	start_time      TEXT NOT NULL,
	end_time        TEXT NOT NULL,
	start_unix_ns   INTEGER NOT NULL,
	end_unix_ns     INTEGER NOT NULL,
	status          TEXT NOT NULL,
	progress        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS backfill_batches_batch_id_idx
	ON backfill_batches (batch_id);

CREATE TABLE IF NOT EXISTS dataset_coverage (
	dataset_id    TEXT NOT NULL,
	start_unix_ns INTEGER NOT NULL,
	end_unix_ns   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS dataset_coverage_dataset_idx
	ON dataset_coverage (dataset_id, start_unix_ns);
`
