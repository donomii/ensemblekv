package ensemblekv

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteKV implements KvLike backed by an on-disk SQLite database.
type SQLiteKV struct {
	DefaultOps

	db *sql.DB
	mu sync.RWMutex
}

const (
	sqliteMainTable = `CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value BLOB NOT NULL,
		updated_at INTEGER NOT NULL
	)`
)

// NewSQLiteKVStore creates a SQLite-backed key value store at the provided directory.
func NewSQLiteKVStore(directory string, blockSize, fileSize int64) (*SQLiteKV, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	dbPath := filepath.Join(directory, "kv.sqlite")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}

	for _, stmt := range []string{sqliteMainTable} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialise sqlite schema: %w", err)
		}
	}

	return &SQLiteKV{
		db: db,
	}, nil
}

func (s *SQLiteKV) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withTx(func(tx *sql.Tx) error {
		now := time.Now().UnixNano()

		if _, err := tx.Exec(`INSERT INTO kv_store(key, value, updated_at)
			VALUES(?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at`,
			string(key), value, now,
		); err != nil {
			return fmt.Errorf("sqlite put upsert: %w", err)
		}

		return nil
	})
}

func (s *SQLiteKV) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var value []byte
	err := s.db.QueryRow(`SELECT value FROM kv_store WHERE key = ?`, string(key)).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("key not found")
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite get: %w", err)
	}
	return value, nil
}

func (s *SQLiteKV) Exists(key []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var exists bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM kv_store WHERE key = ?)`, string(key)).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

func (s *SQLiteKV) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withTx(func(tx *sql.Tx) error {

		res, err := tx.Exec(`DELETE FROM kv_store WHERE key = ?`, string(key))
		if err != nil {
			return fmt.Errorf("sqlite delete: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("key not found")
		}

		return nil
	})
}

func (s *SQLiteKV) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM kv_store`).Scan(&count); err != nil {
		return 0
	}
	return count
}

func (s *SQLiteKV) getRows(query string, args ...any) (*sql.Rows, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.Query(query, args...)
}

func (s *SQLiteKV) MapFunc(f func([]byte, []byte) error) (map[string]bool, error) {
	// Get keys first to separate iteration from modification
	keys := s.Keys() // Keys() handles its own locking

	visited := make(map[string]bool)

	for _, key := range keys {
		val, err := s.Get(key)
		if err != nil {
			// Key might have been deleted concurrently
			continue
		}

		visited[string(key)] = true

		// Call user function without holding DB lock/rows open
		if err := f(key, val); err != nil {
			return visited, err
		}
	}

	return visited, nil
}

func (s *SQLiteKV) List() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`SELECT key FROM kv_store ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("sqlite list query: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("sqlite list scan: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (s *SQLiteKV) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.db.Exec(`PRAGMA wal_checkpoint(FULL)`); err != nil {
		return fmt.Errorf("sqlite checkpoint: %w", err)
	}
	return nil
}

func (s *SQLiteKV) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *SQLiteKV) KeyHistory(key []byte) ([][]byte, error) {

	return nil, nil
}

func (s *SQLiteKV) MapPrefixFunc(prefix []byte, f func([]byte, []byte) error) (map[string]bool, error) {

	// SQLite supports LIKE operator for prefix matching
	prefixStr := string(prefix) + "%"
	rows, err := s.getRows(`SELECT key, value FROM kv_store WHERE key LIKE ?`, prefixStr)
	if err != nil {
		return nil, fmt.Errorf("sqlite prefix query: %w", err)
	}
	defer rows.Close()

	visited := make(map[string]bool)

	for rows.Next() {
		var k string
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("sqlite prefix scan: %w", err)
		}
		if err := f([]byte(k), v); err != nil {
			return visited, err
		}
		visited[k] = true
	}

	if err := rows.Err(); err != nil {
		return visited, fmt.Errorf("sqlite prefix rows: %w", err)
	}

	return visited, nil
}

func (s *SQLiteKV) withTx(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("commit transaction: %w", commitErr)
	}
	return nil
}

func (s *SQLiteKV) Keys() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.keys()
}

func (s *SQLiteKV) keys() [][]byte {
	var keys [][]byte
	rows, err := s.db.Query(`SELECT key FROM kv_store`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil
		}
		keys = append(keys, []byte(key))
	}
	return keys
}
