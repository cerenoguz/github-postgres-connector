package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
	"github.com/cerenoguz/github-postgres-connector/internal/connector"
	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

const repo = "acme/widgets"

// fakeGitHub imitates the three endpoints the connector uses for one
// repository. Its pagination links carry an opaque "after" parameter rather
// than GitHub's "page", so a client that built page URLs itself would fail.
type fakeGitHub struct {
	*httptest.Server

	mu       sync.Mutex
	history  []string // SHAs on the default branch, oldest first
	empty    bool     // the repository has no commits
	requests []string
	// intercept may answer a request before the normal handler; it returns
	// true when it did.
	intercept func(w http.ResponseWriter, r *http.Request, n int) bool
}

func newFakeGitHub(t *testing.T, history ...string) *fakeGitHub {
	t.Helper()
	f := &fakeGitHub{history: history}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeGitHub) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.RequestURI())
	n := len(f.requests)
	f.mu.Unlock()

	if f.intercept != nil && f.intercept(w, r, n) {
		return
	}

	const prefix = "/repos/" + repo
	path, ok := strings.CutPrefix(r.URL.Path, prefix)
	if !ok {
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
		return
	}

	switch {
	case path == "/commits":
		if f.empty {
			http.Error(w, `{"message":"Git Repository is empty."}`, http.StatusConflict)
			return
		}
		// Newest first, starting at ?sha= or at the head.
		list := slices.Clone(f.history)
		if sha := r.URL.Query().Get("sha"); sha != "" {
			i := slices.Index(list, sha)
			if i < 0 {
				http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
				return
			}
			list = list[:i+1]
		}
		slices.Reverse(list)
		f.writePage(w, r, list, func(page []map[string]any) any { return page })

	case strings.HasPrefix(path, "/compare/"):
		base, head, _ := strings.Cut(strings.TrimPrefix(path, "/compare/"), "...")
		from, to := slices.Index(f.history, base), slices.Index(f.history, head)
		if from < 0 || to < 0 {
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
			return
		}
		// Oldest first, like git log base..head --reverse.
		f.writePage(w, r, f.history[from+1:to+1], func(page []map[string]any) any {
			return map[string]any{"status": "ahead", "commits": page}
		})

	case strings.HasPrefix(path, "/commits/"):
		c := commitJSON(strings.TrimPrefix(path, "/commits/"))
		c["stats"] = map[string]any{"additions": 10, "deletions": 2}
		json.NewEncoder(w).Encode(c)

	default:
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
	}
}

func (f *fakeGitHub) writePage(w http.ResponseWriter, r *http.Request, shas []string, wrap func([]map[string]any) any) {
	q := r.URL.Query()
	size, _ := strconv.Atoi(q.Get("per_page"))
	if size == 0 {
		size = 30
	}
	after, _ := strconv.Atoi(q.Get("after"))
	end := min(after+size, len(shas))

	if end < len(shas) {
		q.Set("after", strconv.Itoa(end))
		w.Header().Set("Link", fmt.Sprintf(`<%s%s?%s>; rel="next", <%s/last>; rel="last"`,
			f.URL, r.URL.Path, q.Encode(), f.URL))
	}
	page := []map[string]any{}
	for _, sha := range shas[after:end] {
		page = append(page, commitJSON(sha))
	}
	json.NewEncoder(w).Encode(wrap(page))
}

func commitJSON(sha string) map[string]any {
	sig := map[string]any{"name": "Ada", "email": "ada@example.test", "date": "2026-03-01T10:00:00Z"}
	return map[string]any{
		"sha":       sha,
		"html_url":  "https://github.test/" + repo + "/commit/" + sha,
		"commit":    map[string]any{"message": "commit " + sha, "author": sig, "committer": sig},
		"author":    map[string]any{"login": "ada"},
		"committer": nil,
		"parents":   []any{map[string]any{"sha": "parent-of-" + sha}},
	}
}

func (f *fakeGitHub) requested() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// harness wires a connector to a fake GitHub and records what it emits and
// how long its HTTP client was asked to wait.
type harness struct {
	conn    *Connector
	emitted []string
	pages   []int
	sleeps  []time.Duration
	logs    bytes.Buffer
}

func newHarness(t *testing.T, f *fakeGitHub, cfg Config, authn auth.Authenticator) *harness {
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
	f := newFakeGitHub(t, "c1", "c2", "c3", "c4", "c5")
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
	if got := f.requested(); !slices.Equal(got, want) {
		t.Errorf("requests =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestIncrementalSyncFetchesOnlyCommitsSinceTheCursor(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2", "c3", "c4", "c5", "c6")
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
	if got := f.requested(); !slices.Equal(got, want) {
		t.Errorf("requests =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestNothingNewCostsOneRequest(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2")
	h := newHarness(t, f, Config{}, nil)

	next := h.fetch(t, "c2")

	if len(h.emitted) != 0 || next != "c2" {
		t.Errorf("emitted = %v, cursor = %q; want nothing and an unchanged cursor", h.emitted, next)
	}
	if got := f.requested(); len(got) != 1 {
		t.Errorf("requests = %v, want only the head lookup", got)
	}
}

func TestVanishedCursorFallsBackToTheFullHistory(t *testing.T) {
	// The branch was force-pushed: the old head is no longer in the repository.
	f := newFakeGitHub(t, "n1", "n2", "n3")
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
	f := newFakeGitHub(t)
	f.empty = true
	h := newHarness(t, f, Config{}, nil)

	next := h.fetch(t, "")

	if len(h.emitted) != 0 || next != "" {
		t.Errorf("emitted = %v, cursor = %q; want nothing", h.emitted, next)
	}
}

func TestFetchStatsLoadsAdditionsAndDeletions(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2")
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
	if reqs := f.requested(); !slices.Contains(reqs, "/repos/acme/widgets/commits/c1") ||
		!slices.Contains(reqs, "/repos/acme/widgets/commits/c2") {
		t.Errorf("requests = %v, want one detail request per commit", reqs)
	}
}

func TestStatsAreNotRequestedByDefault(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2")
	h := newHarness(t, f, Config{}, nil)

	h.fetch(t, "")

	if got := f.requested(); len(got) != 2 {
		t.Errorf("requests = %v, want the head lookup and one page", got)
	}
}

func TestUnknownRepositoryFailsWithoutRetrying(t *testing.T) {
	f := newFakeGitHub(t, "c1")
	h := newHarness(t, f, Config{Repositories: []string{"acme/missing"}}, nil)

	_, err := h.conn.Fetch(context.Background(), "acme/missing", "", h.emit)

	var se *httpx.StatusError
	if !errors.As(err, &se) || se.Status != http.StatusNotFound {
		t.Fatalf("err = %v, want a 404", err)
	}
	if got := f.requested(); len(got) != 1 || len(h.sleeps) != 0 {
		t.Errorf("requests = %v, sleeps = %v; want a single attempt", got, h.sleeps)
	}
}

func TestEmitFailureStopsTheWalk(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2", "c3", "c4")
	h := newHarness(t, f, Config{PerPage: 2}, nil)
	boom := errors.New("database is down")

	_, err := h.conn.Fetch(context.Background(), repo, "", func(context.Context, connector.Batch) error { return boom })

	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the emit error", err)
	}
	if got := f.requested(); len(got) != 2 {
		t.Errorf("requests = %v, want no page fetched after the failure", got)
	}
}

func TestTransientErrorMidWalkIsRetriedAndNothingIsLost(t *testing.T) {
	f := newFakeGitHub(t, "c1", "c2", "c3", "c4")
	f.intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
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
	f := newFakeGitHub(t, "c1")
	f.intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
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
	f := newFakeGitHub(t, "c1")
	f.intercept = func(w http.ResponseWriter, _ *http.Request, _ int) bool {
		w.Header().Set("X-RateLimit-Remaining", "4999")
		http.Error(w, `{"message":"Resource not accessible by personal access token"}`, http.StatusForbidden)
		return true
	}
	h := newHarness(t, f, Config{}, nil)

	_, err := h.conn.Fetch(context.Background(), repo, "", h.emit)

	if err == nil {
		t.Fatal("expected an error")
	}
	if got := f.requested(); len(got) != 1 || len(h.sleeps) != 0 {
		t.Errorf("requests = %v, sleeps = %v; want a single attempt", got, h.sleeps)
	}
}

func TestTokenIsSentButNeverLoggedOrReturned(t *testing.T) {
	const token = "ghp_supersecret0123456789"
	var sawToken bool
	f := newFakeGitHub(t, "c1", "c2")
	f.intercept = func(w http.ResponseWriter, r *http.Request, n int) bool {
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
	f := newFakeGitHub(t, "c1", "c2", "c3")
	f.intercept = func(w http.ResponseWriter, _ *http.Request, n int) bool {
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
