package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
	"github.com/cerenoguz/github-postgres-connector/internal/store/storetest"
)

// These are integration tests: they run against a real PostgreSQL started in
// Docker. They are skipped with -short and when Docker is not available.

// newStore returns a store on a migrated, empty database.
func newStore(t *testing.T) *Store {
	t.Helper()
	dsn := storetest.DSN(t)
	if _, err := Migrate(dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s, err := Open(context.Background(), dsn, 10*time.Second)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(context.Background(), `TRUNCATE commits, sync_cursors`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func commit(repo, sha string) connector.Commit {
	return connector.Commit{
		SHA:         sha,
		Repository:  repo,
		Message:     "message of " + sha,
		Author:      connector.Person{Name: "Ada", Email: "ada@example.test", Login: "ada"},
		Committer:   connector.Person{Name: "Web Flow", Email: "noreply@example.test"},
		AuthoredAt:  time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC),
		CommittedAt: time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC),
		URL:         "https://example.test/" + repo + "/commit/" + sha,
		ParentCount: 1,
	}
}

func countCommits(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM commits`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrateIsRepeatable(t *testing.T) {
	dsn := storetest.DSN(t)

	first, err := Migrate(dsn)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Migrate(dsn)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if first != 2 || second != 2 {
		t.Errorf("versions = %d, %d; want 2 both times", first, second)
	}
}

func TestSaveBatchIsIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	batch := connector.Batch{Page: 1, Commits: []connector.Commit{commit("a/b", "sha1"), commit("a/b", "sha2")}}

	first, err := s.SaveBatch(ctx, "github", batch)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SaveBatch(ctx, "github", batch)
	if err != nil {
		t.Fatal(err)
	}
	// A page that overlaps the previous one, as an incremental run produces.
	overlap := connector.Batch{Page: 1, Commits: []connector.Commit{commit("a/b", "sha2"), commit("a/b", "sha3")}}
	third, err := s.SaveBatch(ctx, "github", overlap)
	if err != nil {
		t.Fatal(err)
	}

	if first != 2 || second != 0 || third != 1 {
		t.Errorf("inserted = %d, %d, %d; want 2, 0, 1", first, second, third)
	}
	if n := countCommits(t, s); n != 3 {
		t.Errorf("rows = %d, want 3", n)
	}
}

func TestSameSHAInAnotherRepositoryOrSourceIsADifferentCommit(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	for _, tc := range []struct{ source, repo string }{
		{"github", "a/b"},
		{"github", "a/fork"},
		{"gitlab", "a/b"},
	} {
		n, err := s.SaveBatch(ctx, tc.source, connector.Batch{Commits: []connector.Commit{commit(tc.repo, "sha1")}})
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%s %s: inserted = %d, want 1", tc.source, tc.repo, n)
		}
	}
}

func TestSaveBatchRoundTripsEveryField(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	additions, deletions := 12, 0
	withStats := commit("a/b", "with-stats")
	withStats.Additions, withStats.Deletions = &additions, &deletions
	withStats.ParentCount = 2
	plain := commit("a/b", "plain")

	if _, err := s.SaveBatch(ctx, "github", connector.Batch{Commits: []connector.Commit{withStats, plain}}); err != nil {
		t.Fatal(err)
	}

	read := func(sha string) (c connector.Commit, authorLogin, committerLogin *string) {
		err := s.pool.QueryRow(ctx, `
			SELECT repository, sha, message, author_name, author_email, author_login,
			       committer_name, committer_email, committer_login,
			       authored_at, committed_at, url, parent_count, additions, deletions
			FROM commits WHERE source = 'github' AND sha = $1`, sha,
		).Scan(&c.Repository, &c.SHA, &c.Message, &c.Author.Name, &c.Author.Email, &authorLogin,
			&c.Committer.Name, &c.Committer.Email, &committerLogin,
			&c.AuthoredAt, &c.CommittedAt, &c.URL, &c.ParentCount, &c.Additions, &c.Deletions)
		if err != nil {
			t.Fatalf("read %s: %v", sha, err)
		}
		return c, authorLogin, committerLogin
	}

	got, authorLogin, committerLogin := read("with-stats")
	if got.Message != withStats.Message || got.URL != withStats.URL || got.ParentCount != 2 ||
		got.Author.Name != "Ada" || got.Author.Email != "ada@example.test" ||
		got.Committer.Name != "Web Flow" || got.Committer.Email != "noreply@example.test" {
		t.Errorf("stored commit = %+v", got)
	}
	if !got.AuthoredAt.Equal(withStats.AuthoredAt) || !got.CommittedAt.Equal(withStats.CommittedAt) {
		t.Errorf("dates = %s, %s", got.AuthoredAt, got.CommittedAt)
	}
	if authorLogin == nil || *authorLogin != "ada" {
		t.Errorf("author_login = %v, want ada", authorLogin)
	}
	if committerLogin != nil {
		t.Errorf("committer_login = %q, want NULL for an unlinked committer", *committerLogin)
	}
	if got.Additions == nil || *got.Additions != 12 || got.Deletions == nil || *got.Deletions != 0 {
		t.Errorf("stats = %v, %v; want 12, 0", got.Additions, got.Deletions)
	}

	got, _, _ = read("plain")
	if got.Additions != nil || got.Deletions != nil {
		t.Errorf("stats = %v, %v; want NULL when not fetched", got.Additions, got.Deletions)
	}
}

func TestSaveBatchIsAllOrNothing(t *testing.T) {
	s := newStore(t)
	// PostgreSQL rejects a NUL byte in text, so the second row fails after
	// the first was accepted.
	bad := commit("a/b", "sha2")
	bad.Message = "broken\x00message"
	batch := connector.Batch{Commits: []connector.Commit{commit("a/b", "sha1"), bad}}

	if _, err := s.SaveBatch(context.Background(), "github", batch); err == nil {
		t.Fatal("expected the batch to fail")
	}

	if n := countCommits(t, s); n != 0 {
		t.Errorf("rows = %d, want the whole batch rolled back", n)
	}
}

func TestSaveBatchWithCancelledContextWritesNothing(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.SaveBatch(ctx, "github", connector.Batch{Commits: []connector.Commit{commit("a/b", "sha1")}}); err == nil {
		t.Fatal("expected an error")
	}

	if n := countCommits(t, s); n != 0 {
		t.Errorf("rows = %d, want 0", n)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	if c, err := s.Cursor(ctx, "github", "a/b"); err != nil || c != "" {
		t.Fatalf("unknown resource: cursor = %q, err = %v; want empty", c, err)
	}

	for _, want := range []connector.Cursor{"2026-03-01T11:00:00Z", "2026-03-02T09:30:00Z"} {
		if err := s.SaveCursor(ctx, "github", "a/b", want); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Cursor(ctx, "github", "a/b"); err != nil || got != want {
			t.Errorf("cursor = %q, err = %v; want %q", got, err, want)
		}
	}

	// Cursors are per source and per resource.
	if c, _ := s.Cursor(ctx, "github", "a/other"); c != "" {
		t.Errorf("other resource cursor = %q, want empty", c)
	}
	if c, _ := s.Cursor(ctx, "gitlab", "a/b"); c != "" {
		t.Errorf("other source cursor = %q, want empty", c)
	}
}

func TestOperationsTimeOutInsteadOfHanging(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// Another session holds an exclusive lock on the table, the way a stuck
	// migration or a manual maintenance command would.
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err := blocker.Exec(ctx, `LOCK TABLE commits IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	s.timeout = 200 * time.Millisecond

	start := time.Now()
	_, err = s.SaveBatch(ctx, "github", connector.Batch{Commits: []connector.Commit{commit("a/b", "sha1")}})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a deadline error", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("gave up after %s, want it bounded by the timeout", elapsed)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countCommits(t, s); n != 0 {
		t.Errorf("rows = %d, want nothing written by the timed-out batch", n)
	}
}

func TestOpenRejectsANonPositiveTimeout(t *testing.T) {
	if _, err := Open(context.Background(), "postgres://localhost/app", 0); err == nil {
		t.Error("expected an error")
	}
}
