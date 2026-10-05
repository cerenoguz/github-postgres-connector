// Package store persists synced records and cursors in PostgreSQL.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
)

type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database at dsn and verifies the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("configure database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Cursor returns the stored cursor of a resource, or the zero Cursor if it
// has never been synced.
func (s *Store) Cursor(ctx context.Context, source, resource string) (connector.Cursor, error) {
	var c string
	err := s.pool.QueryRow(ctx,
		`SELECT cursor FROM sync_cursors WHERE source = $1 AND resource = $2`,
		source, resource,
	).Scan(&c)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("select cursor: %w", err)
	}
	return connector.Cursor(c), nil
}

func (s *Store) SaveCursor(ctx context.Context, source, resource string, c connector.Cursor) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sync_cursors (source, resource, cursor)
		VALUES ($1, $2, $3)
		ON CONFLICT (source, resource)
		DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
		source, resource, string(c),
	)
	if err != nil {
		return fmt.Errorf("upsert cursor: %w", err)
	}
	return nil
}

const insertCommit = `
	INSERT INTO commits (
		source, repository, sha, message,
		author_name, author_email, author_login,
		committer_name, committer_email, committer_login,
		authored_at, committed_at, url, parent_count, additions, deletions
	) VALUES (
		$1, $2, $3, $4,
		$5, $6, NULLIF($7, ''),
		$8, $9, NULLIF($10, ''),
		$11, $12, $13, $14, $15, $16
	)
	ON CONFLICT (source, repository, sha) DO NOTHING`

// SaveBatch stores a batch in one transaction and returns how many rows were
// new. Commits are immutable, so a row that already exists is left as it is;
// that makes re-running a sync, or re-sending a page after a crash, harmless.
func (s *Store) SaveBatch(ctx context.Context, source string, b connector.Batch) (int, error) {
	if len(b.Commits) == 0 {
		return 0, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	// A no-op once the transaction is committed.
	defer tx.Rollback(context.WithoutCancel(ctx))

	// All inserts travel in one round trip.
	batch := &pgx.Batch{}
	for _, c := range b.Commits {
		batch.Queue(insertCommit,
			source, c.Repository, c.SHA, c.Message,
			c.Author.Name, c.Author.Email, c.Author.Login,
			c.Committer.Name, c.Committer.Email, c.Committer.Login,
			c.AuthoredAt, c.CommittedAt, c.URL, c.ParentCount, c.Additions, c.Deletions,
		)
	}
	results := tx.SendBatch(ctx, batch)
	inserted := 0
	for _, c := range b.Commits {
		tag, err := results.Exec()
		if err != nil {
			results.Close()
			return 0, fmt.Errorf("insert commit %s: %w", c.SHA, err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := results.Close(); err != nil {
		return 0, fmt.Errorf("insert commits: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return inserted, nil
}
