package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/cerenoguz/github-postgres-connector/internal/connectors/github/githubtest"
	"github.com/cerenoguz/github-postgres-connector/internal/engine"
	"github.com/cerenoguz/github-postgres-connector/internal/store"
	"github.com/cerenoguz/github-postgres-connector/internal/store/storetest"
)

// These tests run the real command against a fake GitHub and a real
// PostgreSQL, so they cover the whole path from config file to stored rows.

const testToken = "ghp_endtoendsecret0123456789"

type env struct {
	t      *testing.T
	github *githubtest.Fake
	db     *pgx.Conn
	config string
}

// newEnv prepares an empty database, a fake GitHub holding the given history
// and a config file that syncs repos from it.
func newEnv(t *testing.T, repos []string, history ...string) *env {
	t.Helper()
	dsn := storetest.DSN(t)
	if _, err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(context.Background()) })
	if _, err := db.Exec(context.Background(), `TRUNCATE commits, sync_cursors`); err != nil {
		t.Fatal(err)
	}

	fake := githubtest.New(t, history...)
	t.Setenv("TEST_DATABASE_URL", dsn)
	t.Setenv("TEST_GITHUB_TOKEN", testToken)

	var list strings.Builder
	for _, r := range repos {
		fmt.Fprintf(&list, "      - %s\n", r)
	}
	cfg := fmt.Sprintf(`
database:
  url: ${TEST_DATABASE_URL}
log:
  level: debug
http:
  timeout: 5s
  retry:
    max_retries: 2
    base_delay: 1ms
    max_delay: 1ms
connectors:
  - type: github
    base_url: %s
    auth:
      token: ${TEST_GITHUB_TOKEN}
    per_page: 2
    repositories:
%s`, fake.URL, list.String())
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return &env{t: t, github: fake, db: db, config: path}
}

// sync runs `connector sync` and returns its exit code and output.
func (e *env) sync(ctx context.Context, extra ...string) (code int, stdout, stderr string) {
	e.t.Helper()
	var out, errOut bytes.Buffer
	args := append([]string{"sync", "--config", e.config}, extra...)
	code = Run(ctx, args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) commits() int { return e.count(`SELECT count(*) FROM commits`) }

func (e *env) cursor(repo string) string {
	e.t.Helper()
	var c string
	err := e.db.QueryRow(context.Background(),
		`SELECT cursor FROM sync_cursors WHERE source = 'github' AND resource = $1`, repo).Scan(&c)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// row extracts "status read inserted" for a repository from the report.
func row(t *testing.T, stdout, repo string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^github\s+` + regexp.QuoteMeta(repo) + `\s+(\w+)\s+(\d+)\s+(\d+)\s`).FindStringSubmatch(stdout)
	if m == nil {
		t.Fatalf("no report row for %s in:\n%s", repo, stdout)
	}
	return strings.Join(m[1:], " ")
}

func TestSyncFullThenIncrementalThenIdempotent(t *testing.T) {
	e := newEnv(t, []string{githubtest.Repo}, "c1", "c2", "c3", "c4", "c5")
	ctx := context.Background()

	// First run: the whole history, across three pages.
	code, stdout, stderr := e.sync(ctx)
	if code != ExitOK {
		t.Fatalf("first run exit = %d\n%s", code, stderr)
	}
	if got := row(t, stdout, githubtest.Repo); got != "ok 5 5" {
		t.Errorf("first run = %q, want ok 5 5", got)
	}
	if e.commits() != 5 || e.cursor(githubtest.Repo) != "c5" {
		t.Errorf("rows = %d, cursor = %q; want 5 and c5", e.commits(), e.cursor(githubtest.Repo))
	}

	// Second run straight away: nothing new, nothing duplicated.
	code, stdout, _ = e.sync(ctx)
	if code != ExitOK || row(t, stdout, githubtest.Repo) != "ok 0 0" || e.commits() != 5 {
		t.Errorf("second run: exit = %d, row = %q, rows = %d; want ok 0 0 and 5 rows",
			code, row(t, stdout, githubtest.Repo), e.commits())
	}

	// Two commits are pushed: only they are fetched.
	e.github.History = append(e.github.History, "c6", "c7")
	code, stdout, _ = e.sync(ctx)
	if code != ExitOK || row(t, stdout, githubtest.Repo) != "ok 2 2" {
		t.Errorf("incremental run: exit = %d, row = %q; want ok 2 2", code, row(t, stdout, githubtest.Repo))
	}
	if e.commits() != 7 || e.cursor(githubtest.Repo) != "c7" {
		t.Errorf("rows = %d, cursor = %q; want 7 and c7", e.commits(), e.cursor(githubtest.Repo))
	}

	// --full re-reads everything and inserts nothing.
	code, stdout, _ = e.sync(ctx, "--full")
	if code != ExitOK || row(t, stdout, githubtest.Repo) != "ok 7 0" || e.commits() != 7 {
		t.Errorf("full run: exit = %d, row = %q, rows = %d; want ok 7 0 and 7 rows",
			code, row(t, stdout, githubtest.Repo), e.commits())
	}
}

func TestOneFailingRepositoryDoesNotBlockTheOthers(t *testing.T) {
	// The fake only knows githubtest.Repo; the other one answers 404.
	e := newEnv(t, []string{"acme/missing", githubtest.Repo}, "c1", "c2", "c3")

	code, stdout, _ := e.sync(context.Background())

	if code != ExitSyncFailed {
		t.Errorf("exit = %d, want %d", code, ExitSyncFailed)
	}
	if got := row(t, stdout, "acme/missing"); got != "failed 0 0" {
		t.Errorf("missing repo = %q, want failed 0 0", got)
	}
	if got := row(t, stdout, githubtest.Repo); got != "ok 3 3" {
		t.Errorf("good repo = %q, want ok 3 3", got)
	}
	if !strings.Contains(stdout, "HTTP 404") {
		t.Errorf("report does not explain the failure:\n%s", stdout)
	}
	if e.cursor("acme/missing") != "" {
		t.Error("a cursor was stored for the failed repository")
	}
}

func TestFailureMidwayIsRecoveredOnTheNextRun(t *testing.T) {
	e := newEnv(t, []string{githubtest.Repo}, "c1", "c2", "c3", "c4", "c5")
	// Requests: 1 head lookup, 2 first page, 3 second page.
	e.github.Intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
		if n == 3 {
			http.Error(w, `{"message":"Bad credentials"}`, http.StatusUnauthorized)
			return true
		}
		return false
	}

	code, stdout, _ := e.sync(context.Background())

	if code != ExitSyncFailed || row(t, stdout, githubtest.Repo) != "failed 2 2" {
		t.Errorf("failing run: exit = %d, row = %q; want failed 2 2", code, row(t, stdout, githubtest.Repo))
	}
	if e.commits() != 2 {
		t.Errorf("rows = %d, want the first page kept", e.commits())
	}
	if e.cursor(githubtest.Repo) != "" {
		t.Error("the cursor advanced although the sync did not finish")
	}

	e.github.Intercept = nil
	code, stdout, _ = e.sync(context.Background())

	if code != ExitOK || row(t, stdout, githubtest.Repo) != "ok 5 3" {
		t.Errorf("recovery run: exit = %d, row = %q; want ok 5 3", code, row(t, stdout, githubtest.Repo))
	}
	if e.commits() != 5 || e.cursor(githubtest.Repo) != "c5" {
		t.Errorf("rows = %d, cursor = %q; want 5 and c5 with nothing lost or duplicated",
			e.commits(), e.cursor(githubtest.Repo))
	}
}

func TestCancellationStopsCleanlyAndTheNextRunCompletes(t *testing.T) {
	e := newEnv(t, []string{githubtest.Repo}, "c1", "c2", "c3", "c4", "c5")
	ctx, cancel := context.WithCancel(context.Background())
	e.github.Intercept = func(_ http.ResponseWriter, _ *http.Request, n int) bool {
		if n == 3 { // Ctrl+C arrives while the second page is in flight
			cancel()
			time.Sleep(50 * time.Millisecond)
		}
		return false
	}

	code, stdout, _ := e.sync(ctx)

	if code != ExitSyncFailed || !strings.HasPrefix(row(t, stdout, githubtest.Repo), "cancelled ") {
		t.Errorf("cancelled run: exit = %d, row = %q", code, row(t, stdout, githubtest.Repo))
	}
	if e.cursor(githubtest.Repo) != "" {
		t.Error("the cursor advanced on a cancelled run")
	}
	if n := e.commits(); n%2 != 0 {
		t.Errorf("rows = %d, want only whole pages of 2", n)
	}

	e.github.Intercept = nil
	code, _, _ = e.sync(context.Background())

	if code != ExitOK || e.commits() != 5 || e.cursor(githubtest.Repo) != "c5" {
		t.Errorf("next run: exit = %d, rows = %d, cursor = %q; want a complete sync",
			code, e.commits(), e.cursor(githubtest.Repo))
	}
}

func TestTokenNeverAppearsInOutput(t *testing.T) {
	e := newEnv(t, []string{"acme/missing", githubtest.Repo}, "c1", "c2", "c3")
	sent := false
	e.github.Intercept = func(w http.ResponseWriter, r *http.Request, n int) bool {
		sent = sent || r.Header.Get("Authorization") == "Bearer "+testToken
		if n == 2 { // force a logged retry as well as the 404 failure
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		return false
	}

	_, stdout, stderr := e.sync(context.Background())

	if !sent {
		t.Fatal("the token was never sent, so this test proves nothing")
	}
	if !strings.Contains(stderr, "retrying request") {
		t.Fatalf("expected debug logs with a retry in stderr:\n%s", stderr)
	}
	if strings.Contains(stdout, testToken) || strings.Contains(stderr, testToken) {
		t.Errorf("the token leaked:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
}

func TestSetupErrorsExitWithTheirOwnCode(t *testing.T) {
	t.Run("missing config file", func(t *testing.T) {
		var out, errOut bytes.Buffer
		code := Run(context.Background(), []string{"sync", "--config", "/does/not/exist.yaml"}, &out, &errOut)
		if code != ExitSetup || !strings.Contains(errOut.String(), "read config") {
			t.Errorf("exit = %d, stderr = %s", code, errOut.String())
		}
	})
	t.Run("unknown connector type", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		cfg := "database:\n  url: postgres://localhost:1/nothing\nconnectors:\n  - type: subversion\n"
		if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
		var out, errOut bytes.Buffer
		code := Run(context.Background(), []string{"sync", "--config", path}, &out, &errOut)
		// The database URL points nowhere: failing on the type first shows
		// that config mistakes are caught before the database is touched.
		if code != ExitSetup || !strings.Contains(errOut.String(), `unknown type "subversion"`) {
			t.Errorf("exit = %d, stderr = %s", code, errOut.String())
		}
	})
	t.Run("unknown flag", func(t *testing.T) {
		var out, errOut bytes.Buffer
		if code := Run(context.Background(), []string{"sync", "--nope"}, &out, &errOut); code != ExitSetup {
			t.Errorf("exit = %d, want %d", code, ExitSetup)
		}
	})
}

func TestWriteReport(t *testing.T) {
	report := engine.Report{Results: []engine.Result{
		{Source: "github", Resource: "acme/widgets", Read: 120, Inserted: 20, Duration: 1234 * time.Millisecond},
		{Source: "github", Resource: "acme/broken", Read: 100, Inserted: 100, Err: errors.New("page 2: HTTP 502")},
		{Source: "github", Resource: "acme/later", Err: context.Canceled},
	}}
	var out bytes.Buffer

	writeReport(&out, report)

	want := `CONNECTOR  REPOSITORY    STATUS     READ  INSERTED  DURATION
github     acme/widgets  ok         120   20        1.234s
github     acme/broken   failed     100   100       0s
github     acme/later    cancelled  0     0         0s

3 repositories, 2 failed, 220 commits read, 120 inserted

Errors:
  github acme/broken: page 2: HTTP 502
  github acme/later: context canceled
`
	if out.String() != want {
		t.Errorf("report =\n%s\nwant\n%s", out.String(), want)
	}
}
