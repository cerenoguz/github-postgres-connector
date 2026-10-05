// Package githubtest provides a fake GitHub API server for tests.
package githubtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Repo is the only repository the fake knows; any other answers 404.
const Repo = "acme/widgets"

// Fake imitates the three endpoints the connector uses, for the single
// repository Repo. Its pagination links carry an opaque "after" parameter rather
// than GitHub's "page", so a client that built page URLs itself would fail.
type Fake struct {
	*httptest.Server

	mu       sync.Mutex
	requests []string
	// History holds the SHAs on the default branch, oldest first. Append to
	// it between syncs to simulate pushes.
	History []string
	// Empty makes the repository answer as one without commits.
	Empty bool
	// Intercept may answer request number n (starting at 1) before the
	// normal handler; it returns true when it did.
	Intercept func(w http.ResponseWriter, r *http.Request, n int) bool
}

func New(t *testing.T, history ...string) *Fake {
	t.Helper()
	f := &Fake{History: history}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.RequestURI())
	n := len(f.requests)
	f.mu.Unlock()

	if f.Intercept != nil && f.Intercept(w, r, n) {
		return
	}

	const prefix = "/repos/" + Repo
	path, ok := strings.CutPrefix(r.URL.Path, prefix)
	if !ok {
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
		return
	}

	switch {
	case path == "/commits":
		if f.Empty {
			http.Error(w, `{"message":"Git Repository is empty."}`, http.StatusConflict)
			return
		}
		// Newest first, starting at ?sha= or at the head.
		list := slices.Clone(f.History)
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
		from, to := slices.Index(f.History, base), slices.Index(f.History, head)
		if from < 0 || to < 0 {
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
			return
		}
		// Oldest first, like git log base..head --reverse.
		f.writePage(w, r, f.History[from+1:to+1], func(page []map[string]any) any {
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

func (f *Fake) writePage(w http.ResponseWriter, r *http.Request, shas []string, wrap func([]map[string]any) any) {
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
		"html_url":  "https://github.test/" + Repo + "/commit/" + sha,
		"commit":    map[string]any{"message": "commit " + sha, "author": sig, "committer": sig},
		"author":    map[string]any{"login": "ada"},
		"committer": nil,
		"parents":   []any{map[string]any{"sha": "parent-of-" + sha}},
	}
}

// Requested returns the URI of every request received so far.
func (f *Fake) Requested() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}
