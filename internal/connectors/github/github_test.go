package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
	"github.com/cerenoguz/github-postgres-connector/internal/connector"
	"github.com/cerenoguz/github-postgres-connector/internal/connectors/github/githubtest"
	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

const repo = githubtest.Repo

// harness wires a connector to a fake GitHub and records what it emits and
// how long its HTTP client was asked to wait.
type harness struct {
	conn    *Connector
	emitted []string
	pages   []int
	sleeps  []time.Duration
	logs    bytes.Buffer
}

func newHarness(t *testing.T, f *githubtest.Fake, cfg Config, authn auth.Authenticator) *harness {
	t.Helper()
	h := &harness{}
	log := slog.New(slog.NewJSONHandler(&h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := httpx.New(httpx.Options{
		Auth:     authn,
		Classify: Classifier,
		Headers:  Headers,
		Retry:    httpx.RetryPolicy{MaxRetries: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, MaxWait: time.Hour},
		Logger:   log,
		Sleep: func(_ context.Context, d time.Duration) error {
			h.sleeps = append(h.sleeps, d)
			return nil
		},
	})
	cfg.BaseURL = f.URL
	if cfg.Repositories == nil {
		cfg.Repositories = []string{repo}
	}
	conn, err := New(cfg, client, log)
	if err != nil {
		t.Fatal(err)
	}
	h.conn = conn
	return h
}

func (h *harness) emit(_ context.Context, b connector.Batch) error {
	h.pages = append(h.pages, b.Page)
	for _, c := range b.Commits {
		h.emitted = append(h.emitted, c.SHA)
	}
	return nil
}

func (h *harness) fetch(t *testing.T, since connector.Cursor) connector.Cursor {
	t.Helper()
	next, err := h.conn.Fetch(context.Background(), repo, since, h.emit)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	return next
}

func TestFirstSyncWalksEveryPageOfTheHistory(t *testing.T) {
	f := githubtest.New(t, "c1", "c2", "c3", "c4", "c5")
	h := newHarness(t, f, Config{PerPage: 2}, nil)

	next := h.fetch(t, "")

	if got := strings.Join(h.emitted, ","); got != "c5,c4,c3,c2,c1" {
		t.Errorf("emitted = %s, want every commit", got)
	}
	if !slices.Equal(h.pages, []int{1, 2, 3}) {
		t.Errorf("pages = %v, want 1 2 3", h.pages)
	}
	if next != "c5" {
		t.Errorf("cursor = %q, want the head c5", next)
	}
	want := []string{
		"/repos/acme/widgets/commits?per_page=1",
		"/repos/acme/widgets/commits?per_page=2&sha=c5",
		"/repos/acme/widgets/commits?after=2&per_page=2&sha=c5",
		"/repos/acme/widgets/commits?after=4&per_page=2&sha=c5",
	}
	if got := f.Requested(); !slices.Equal(got, want) {
		t.Errorf("requests =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestIncrementalSyncFetchesOnlyCommitsSinceTheCursor(t *testing.T) {
	f := githubtest.New(t, "c1", "c2", "c3", "c4", "c5", "c6")
	h := newHarness(t, f, Config{PerPage: 2}, nil)

	next := h.fetch(t, "c3")

	if got := strings.Join(h.emitted, ","); got != "c4,c5,c6" {
		t.Errorf("emitted = %s, want only c4,c5,c6", got)
	}
	if next != "c6" {
		t.Errorf("cursor = %q, want c6", next)
	}
	want := []string{
		"/repos/acme/widgets/commits?per_page=1",
		"/repos/acme/widgets/compare/c3...c6?per_page=2",
		"/repos/acme/widgets/compare/c3...c6?after=2&per_page=2",
	}
	if got := f.Requested(); !slices.Equal(got, want) {
		t.Errorf("requests =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestNothingNewCostsOneRequest(t *testing.T) {
	f := githubtest.New(t, "c1", "c2")
	h := newHarness(t, f, Config{}, nil)

	next := h.fetch(t, "c2")

	if len(h.emitted) != 0 || next != "c2" {
		t.Errorf("emitted = %v, cursor = %q; want nothing and an unchanged cursor", h.emitted, next)
	}
	if got := f.Requested(); len(got) != 1 {
		t.Errorf("requests = %v, want only the head lookup", got)
	}
}

func TestVanishedCursorFallsBackToTheFullHistory(t *testing.T) {
	// The branch was force-pushed: the old head is no longer in the repository.
	f := githubtest.New(t, "n1", "n2", "n3")
	h := newHarness(t, f, Config{}, nil)

	next := h.fetch(t, "old-head")

	if got := strings.Join(h.emitted, ","); got != "n3,n2,n1" {
		t.Errorf("emitted = %s, want the full new history", got)
	}
	if next != "n3" {
		t.Errorf("cursor = %q, want n3", next)
	}
	if !strings.Contains(h.logs.String(), "previous head no longer exists") {
		t.Error("the fallback was not logged")
	}
}

func TestEmptyRepositoryIsNotAnError(t *testing.T) {
	f := githubtest.New(t)
	f.Empty = true
	h := newHarness(t, f, Config{}, nil)

	next := h.fetch(t, "")

	if len(h.emitted) != 0 || next != "" {
		t.Errorf("emitted = %v, cursor = %q; want nothing", h.emitted, next)
	}
}

func TestFetchStatsLoadsAdditionsAndDeletions(t *testing.T) {
	f := githubtest.New(t, "c1", "c2")
	h := newHarness(t, f, Config{FetchStats: true}, nil)
	var got []connector.Commit

	_, err := h.conn.Fetch(context.Background(), repo, "", func(_ context.Context, b connector.Batch) error {
		got = append(got, b.Commits...)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d commits, want 2", len(got))
	}
	for _, c := range got {
		if c.Additions == nil || *c.Additions != 10 || c.Deletions == nil || *c.Deletions != 2 {
			t.Errorf("%s stats = %v, %v; want 10, 2", c.SHA, c.Additions, c.Deletions)
		}
	}
	if reqs := f.Requested(); !slices.Contains(reqs, "/repos/acme/widgets/commits/c1") ||
		!slices.Contains(reqs, "/repos/acme/widgets/commits/c2") {
		t.Errorf("requests = %v, want one detail request per commit", reqs)
	}
}

func TestStatsAreNotRequestedByDefault(t *testing.T) {
	f := githubtest.New(t, "c1", "c2")
	h := newHarness(t, f, Config{}, nil)

	h.fetch(t, "")

	if got := f.Requested(); len(got) != 2 {
		t.Errorf("requests = %v, want the head lookup and one page", got)
	}
}

func TestUnknownRepositoryFailsWithoutRetrying(t *testing.T) {
	f := githubtest.New(t, "c1")
	h := newHarness(t, f, Config{Repositories: []string{"acme/missing"}}, nil)

	_, err := h.conn.Fetch(context.Background(), "acme/missing", "", h.emit)

	var se *httpx.StatusError
	if !errors.As(err, &se) || se.Status != http.StatusNotFound {
		t.Fatalf("err = %v, want a 404", err)
	}
	if got := f.Requested(); len(got) != 1 || len(h.sleeps) != 0 {
		t.Errorf("requests = %v, sleeps = %v; want a single attempt", got, h.sleeps)
	}
}

func TestEmitFailureStopsTheWalk(t *testing.T) {
	f := githubtest.New(t, "c1", "c2", "c3", "c4")
	h := newHarness(t, f, Config{PerPage: 2}, nil)
	boom := errors.New("database is down")

	_, err := h.conn.Fetch(context.Background(), repo, "", func(context.Context, connector.Batch) error { return boom })

	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the emit error", err)
	}
	if got := f.Requested(); len(got) != 2 {
		t.Errorf("requests = %v, want no page fetched after the failure", got)
	}
}

func TestTransientErrorMidWalkIsRetriedAndNothingIsLost(t *testing.T) {
	f := githubtest.New(t, "c1", "c2", "c3", "c4")
	f.Intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
		if n == 3 { // the second page, first time round
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		return false
	}
	h := newHarness(t, f, Config{PerPage: 2}, nil)

	h.fetch(t, "")

	if got := strings.Join(h.emitted, ","); got != "c4,c3,c2,c1" {
		t.Errorf("emitted = %s, want every commit exactly once", got)
	}
	if len(h.sleeps) != 1 {
		t.Errorf("sleeps = %v, want one backoff", h.sleeps)
	}
}

func TestPrimaryRateLimitWaitsForTheReset(t *testing.T) {
	f := githubtest.New(t, "c1")
	f.Intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
		if n == 1 {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10))
			http.Error(w, `{"message":"API rate limit exceeded"}`, http.StatusForbidden)
			return true
		}
		return false
	}
	h := newHarness(t, f, Config{}, nil)

	h.fetch(t, "")

	if len(h.emitted) != 1 {
		t.Errorf("emitted = %v, want the sync to finish after the wait", h.emitted)
	}
	if len(h.sleeps) != 1 || h.sleeps[0] < 4*time.Minute+55*time.Second || h.sleeps[0] > 5*time.Minute+2*time.Second {
		t.Errorf("sleeps = %v, want one wait of about 5 minutes", h.sleeps)
	}
}

func TestForbiddenWithoutRateLimitIsNotRetried(t *testing.T) {
	f := githubtest.New(t, "c1")
	f.Intercept = func(w http.ResponseWriter, _ *http.Request, _ int) bool {
		w.Header().Set("X-RateLimit-Remaining", "4999")
		http.Error(w, `{"message":"Resource not accessible by personal access token"}`, http.StatusForbidden)
		return true
	}
	h := newHarness(t, f, Config{}, nil)

	_, err := h.conn.Fetch(context.Background(), repo, "", h.emit)

	if err == nil {
		t.Fatal("expected an error")
	}
	if got := f.Requested(); len(got) != 1 || len(h.sleeps) != 0 {
		t.Errorf("requests = %v, sleeps = %v; want a single attempt", got, h.sleeps)
	}
}

func TestTokenIsSentButNeverLoggedOrReturned(t *testing.T) {
	const token = "ghp_supersecret0123456789"
	var sawToken bool
	f := githubtest.New(t, "c1", "c2")
	f.Intercept = func(w http.ResponseWriter, r *http.Request, n int) bool {
		sawToken = r.Header.Get("Authorization") == "Bearer "+token
		switch n {
		case 1: // a retry, so the retry log line is produced
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		case 3: // then a permanent failure, so an error is returned
			http.Error(w, `{"message":"Bad credentials"}`, http.StatusUnauthorized)
			return true
		}
		return false
	}
	bearer, err := auth.Bearer(auth.NewSecret(token))
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, f, Config{}, bearer)

	_, err = h.conn.Fetch(context.Background(), repo, "", h.emit)

	if !sawToken {
		t.Error("the token was not sent in the Authorization header")
	}
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("error leaks the token: %v", err)
	}
	if h.logs.Len() == 0 {
		t.Fatal("nothing was logged, so the check below proves nothing")
	}
	if strings.Contains(h.logs.String(), token) {
		t.Errorf("logs leak the token:\n%s", h.logs.String())
	}
}

func TestRetryLogsCarryRepositoryAndPage(t *testing.T) {
	f := githubtest.New(t, "c1", "c2", "c3")
	f.Intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
		if n == 3 {
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		return false
	}
	h := newHarness(t, f, Config{PerPage: 2}, nil)

	h.fetch(t, "")

	var retry map[string]any
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		if entry["msg"] == "retrying request" {
			retry = entry
		}
	}
	if retry == nil {
		t.Fatalf("no retry was logged:\n%s", h.logs.String())
	}
	if retry["connector"] != "github" || retry["resource"] != repo || retry["page"] != float64(2) || retry["attempt"] != float64(1) {
		t.Errorf("retry log = %v, want connector, resource, page 2 and attempt 1", retry)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	client := httpx.New(httpx.Options{})
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no repositories", Config{}},
		{"missing owner", Config{Repositories: []string{"widgets"}}},
		{"empty repo", Config{Repositories: []string{"acme/"}}},
		{"too many parts", Config{Repositories: []string{"acme/widgets/extra"}}},
		{"per_page too large", Config{Repositories: []string{repo}, PerPage: 101}},
		{"relative base url", Config{Repositories: []string{repo}, BaseURL: "api.github.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg, client, nil); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
