// Package github is the GitHub connector. It is the only package that knows
// GitHub's URLs, payloads and rate-limit headers.
//
// # Incremental strategy
//
// The cursor is the SHA of the default branch's head at the last successful
// sync. A run resolves the current head and then asks GitHub for exactly the
// commits reachable from the new head but not from the old one
// (GET /compare/{old}...{new}, the equivalent of `git log old..new`).
//
// A cursor based on the date of the last commit would be simpler, but a
// commit's date is when it was written, not when it reached the branch. A
// pull request merged today brings commits dated last week, and filtering by
// date would silently skip them. Comparing heads has no such gap.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

const (
	Name           = "github"
	DefaultBaseURL = "https://api.github.com"
	maxPerPage     = 100
)

// Headers are sent with every GitHub API request.
var Headers = map[string]string{
	"Accept":               "application/vnd.github+json",
	"X-GitHub-Api-Version": "2022-11-28",
	"User-Agent":           "github-postgres-connector",
}

type Config struct {
	// BaseURL is the API root; empty means github.com. GitHub Enterprise
	// Server uses https://HOST/api/v3.
	BaseURL string
	// Repositories are the "owner/repo" names to sync.
	Repositories []string
	// PerPage is the page size, 1 to 100; zero means 100.
	PerPage int
	// FetchStats also loads lines added and deleted. It costs one extra
	// request per commit, so it is off by default.
	FetchStats bool
}

type Connector struct {
	client  *httpx.Client
	baseURL string
	repos   []string
	perPage int
	stats   bool
	log     *slog.Logger
}

// New validates cfg and returns a connector that sends its requests through
// client. The client should be built with Classifier and Headers.
func New(cfg Config, client *httpx.Client, log *slog.Logger) (*Connector, error) {
	if len(cfg.Repositories) == 0 {
		return nil, errors.New("github: no repositories configured")
	}
	for _, r := range cfg.Repositories {
		owner, repo, ok := strings.Cut(r, "/")
		if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
			return nil, fmt.Errorf("github: repository %q is not in owner/repo form", r)
		}
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("github: base URL %q is not an absolute URL", cfg.BaseURL)
	}
	// Plain HTTP would send the token in clear text. It is allowed only for
	// a server on this machine, which is what tests and local proxies use.
	if u.Scheme != "https" && !(u.Scheme == "http" && isLoopback(u.Hostname())) {
		return nil, fmt.Errorf("github: base URL %q must use https", cfg.BaseURL)
	}
	perPage := cfg.PerPage
	if perPage == 0 {
		perPage = maxPerPage
	}
	if perPage < 1 || perPage > maxPerPage {
		return nil, fmt.Errorf("github: per_page must be between 1 and %d, got %d", maxPerPage, cfg.PerPage)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Connector{
		client:  client,
		baseURL: base,
		repos:   cfg.Repositories,
		perPage: perPage,
		stats:   cfg.FetchStats,
		log:     log,
	}, nil
}

func (c *Connector) Name() string        { return Name }
func (c *Connector) Resources() []string { return c.repos }

// Fetch emits the commits of the default branch that are not reachable from
// the since cursor, or the whole history when since is empty.
func (c *Connector) Fetch(ctx context.Context, repo string, since connector.Cursor, emit connector.EmitFunc) (connector.Cursor, error) {
	log := c.log.With("connector", Name, "resource", repo)
	ctx = httpx.WithLogger(ctx, log)

	// Pinning the run to one head SHA keeps pagination stable while people
	// keep pushing, and makes the new cursor exact.
	head, err := c.head(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("resolve head: %w", err)
	}
	if head == "" {
		log.Info("repository has no commits")
		return since, nil
	}
	if head == string(since) {
		log.Info("no new commits", "head", head)
		return since, nil
	}

	if since != "" {
		err := c.fetchRange(ctx, repo, string(since), head, emit)
		if err == nil {
			return connector.Cursor(head), nil
		}
		switch {
		case errors.Is(err, errRangeTruncated):
			log.Warn("too many new commits for one comparison, reading the full history", "previous_head", string(since))
		case isStatus(err, http.StatusNotFound):
			// The repository itself exists (head resolved), so the 404 is
			// about the old head: it was force-pushed away and garbage
			// collected.
			log.Warn("previous head no longer exists, reading the full history", "previous_head", string(since))
		default:
			return "", err
		}
		// Re-reading everything is safe because existing rows are skipped.
	}

	if err := c.fetchAll(ctx, repo, head, emit); err != nil {
		return "", err
	}
	return connector.Cursor(head), nil
}

// head returns the SHA at the tip of the default branch, or "" for a
// repository without commits.
func (c *Connector) head(ctx context.Context, repo string) (string, error) {
	// Without a "sha" parameter the listing starts at the default branch.
	resp, err := c.client.Get(ctx, c.repoURL(repo, "/commits", url.Values{"per_page": {"1"}}))
	if err != nil {
		// GitHub answers 409 "Git Repository is empty" instead of an empty list.
		if isStatus(err, http.StatusConflict) {
			return "", nil
		}
		return "", err
	}
	var commits []apiCommit
	if err := json.Unmarshal(resp.Body, &commits); err != nil {
		return "", fmt.Errorf("decode commit list: %w", err)
	}
	if len(commits) == 0 {
		return "", nil
	}
	return commits[0].SHA, nil
}

// fetchAll walks the whole history reachable from head, newest first.
func (c *Connector) fetchAll(ctx context.Context, repo, head string, emit connector.EmitFunc) error {
	first := c.repoURL(repo, "/commits", url.Values{
		"sha":      {head},
		"per_page": {strconv.Itoa(c.perPage)},
	})
	return c.client.Pages(ctx, first, func(page int, r *httpx.Response) error {
		var commits []apiCommit
		if err := json.Unmarshal(r.Body, &commits); err != nil {
			return fmt.Errorf("page %d: decode commit list: %w", page, err)
		}
		return c.emit(ctx, repo, page, commits, emit)
	})
}

// errRangeTruncated reports a comparison that GitHub cut short.
var errRangeTruncated = errors.New("comparison does not list every commit")

// fetchRange walks the commits reachable from head but not from base.
//
// GitHub lists at most 10,000 commits per comparison and silently drops the
// oldest ones beyond that. The first page says how many commits the range
// really has (ahead_by) and how many will be listed (total_commits); when
// they differ, nothing is emitted and errRangeTruncated is returned so the
// caller can read the history another way.
func (c *Connector) fetchRange(ctx context.Context, repo, base, head string, emit connector.EmitFunc) error {
	first := c.repoURL(repo, "/compare/"+url.PathEscape(base)+"..."+url.PathEscape(head), url.Values{
		"per_page": {strconv.Itoa(c.perPage)},
	})
	return c.client.Pages(ctx, first, func(page int, r *httpx.Response) error {
		var comparison struct {
			AheadBy      int         `json:"ahead_by"`
			TotalCommits int         `json:"total_commits"`
			Commits      []apiCommit `json:"commits"`
		}
		if err := json.Unmarshal(r.Body, &comparison); err != nil {
			return fmt.Errorf("page %d: decode comparison: %w", page, err)
		}
		if page == 1 && comparison.AheadBy > comparison.TotalCommits {
			return fmt.Errorf("%w: %d of %d", errRangeTruncated, comparison.TotalCommits, comparison.AheadBy)
		}
		return c.emit(ctx, repo, page, comparison.Commits, emit)
	})
}

func (c *Connector) emit(ctx context.Context, repo string, page int, commits []apiCommit, emit connector.EmitFunc) error {
	if len(commits) == 0 {
		return nil
	}
	batch := connector.Batch{Page: page, Commits: make([]connector.Commit, 0, len(commits))}
	for _, ac := range commits {
		if c.stats {
			detailed, err := c.commit(ctx, repo, ac.SHA)
			if err != nil {
				return fmt.Errorf("page %d: stats of %s: %w", page, ac.SHA, err)
			}
			ac.Stats = detailed.Stats
		}
		batch.Commits = append(batch.Commits, toCommit(repo, ac))
	}
	return emit(ctx, batch)
}

// commit loads one commit in full; the list endpoints leave out its stats.
func (c *Connector) commit(ctx context.Context, repo, sha string) (apiCommit, error) {
	var out apiCommit
	resp, err := c.client.Get(ctx, c.repoURL(repo, "/commits/"+url.PathEscape(sha), nil))
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return out, fmt.Errorf("decode commit: %w", err)
	}
	return out, nil
}

// repoURL builds the first URL of a request. Later pages are never built
// here: they come from the Link header.
func (c *Connector) repoURL(repo, path string, query url.Values) string {
	owner, name, _ := strings.Cut(repo, "/")
	u := c.baseURL + "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(name) + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return u
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isStatus(err error, status int) bool {
	var se *httpx.StatusError
	return errors.As(err, &se) && se.Status == status
}
