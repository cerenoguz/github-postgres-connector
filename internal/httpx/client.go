// Package httpx is the HTTP client every connector shares. It owns the
// cross-cutting behaviour of talking to a remote API: credentials, per-request
// timeouts, retries with backoff, rate-limit waits and pagination.
//
// Nothing here knows about a particular API. What differs per tool (which
// responses mean "rate limited", and for how long) is supplied as a Classifier.
package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
)

// RetryPolicy bounds how hard the client tries before giving up.
type RetryPolicy struct {
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries int
	// BaseDelay is the backoff before the first retry; it doubles each time.
	BaseDelay time.Duration
	// MaxDelay caps a single backoff.
	MaxDelay time.Duration
	// MaxWait caps a wait the server asked for (Retry-After, rate-limit
	// reset). A longer wait fails the request instead of blocking the run.
	MaxWait time.Duration
}

// DefaultRetryPolicy suits a batch job against a public API.
var DefaultRetryPolicy = RetryPolicy{
	MaxRetries: 5,
	BaseDelay:  500 * time.Millisecond,
	MaxDelay:   30 * time.Second,
	MaxWait:    time.Hour,
}

type Options struct {
	// HTTPClient is the transport to use; nil means a fresh http.Client.
	HTTPClient *http.Client
	// Auth adds credentials to every attempt; nil means none.
	Auth auth.Authenticator
	// Timeout bounds one attempt, including reading the body.
	Timeout time.Duration
	Retry   RetryPolicy
	// Classify decides what a response means; nil means DefaultClassifier.
	Classify Classifier
	// Headers are sent on every request.
	Headers map[string]string
	Logger  *slog.Logger
}

// Response is a fully read successful response.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

type Client struct {
	http     *http.Client
	auth     auth.Authenticator
	timeout  time.Duration
	retry    RetryPolicy
	classify Classifier
	headers  map[string]string
	log      *slog.Logger

	// Replaced in tests to make time and jitter deterministic.
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
	rand  func() float64

	mu        sync.Mutex
	notBefore time.Time // earliest moment the next request may be sent
}

func New(o Options) *Client {
	c := &Client{
		http:     o.HTTPClient,
		auth:     o.Auth,
		timeout:  o.Timeout,
		retry:    o.Retry,
		classify: o.Classify,
		headers:  o.Headers,
		log:      o.Logger,
		now:      time.Now,
		sleep:    sleepContext,
		rand:     rand.Float64,
	}
	if c.http == nil {
		c.http = &http.Client{}
	}
	if c.auth == nil {
		c.auth = auth.None()
	}
	if c.timeout <= 0 {
		c.timeout = 30 * time.Second
	}
	if c.classify == nil {
		c.classify = DefaultClassifier
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	return c
}

// Get fetches url, retrying transient failures and waiting out rate limits as
// the policy and classifier dictate. It returns only 2xx responses; anything
// else comes back as an error wrapping *StatusError.
func (c *Client) Get(ctx context.Context, url string) (*Response, error) {
	log := LoggerFrom(ctx, c.log)

	for attempt := 1; ; attempt++ {
		if err := c.waitUntilAllowed(ctx, log); err != nil {
			return nil, err
		}

		resp, err := c.attempt(ctx, url)

		var cause error
		var wait time.Duration
		switch {
		case err != nil:
			// The caller cancelling is not a failure to retry.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var perm permanentError
			if errors.As(err, &perm) {
				return nil, perm.error
			}
			// Everything else is a transport problem: connection refused or
			// reset, DNS, or this attempt's own timeout.
			cause = err
		default:
			d := c.classify(resp.Status, resp.Header, c.now())
			if resp.Status >= 200 && resp.Status < 300 {
				if d.Wait > 0 {
					c.deferNextRequest(d.Wait)
				}
				return resp, nil
			}
			cause = newStatusError(url, resp)
			if !d.Retry {
				return nil, cause
			}
			wait = d.Wait
		}

		if attempt > c.retry.MaxRetries {
			return nil, fmt.Errorf("giving up after %d attempts: %w", attempt, cause)
		}
		if wait > c.retry.MaxWait {
			return nil, fmt.Errorf("server asked to wait %s, above the %s limit: %w",
				wait.Round(time.Second), c.retry.MaxWait, cause)
		}
		serverAsked := wait > 0
		if !serverAsked {
			wait = c.backoff(attempt)
		}

		log.Warn("retrying request",
			"attempt", attempt,
			"max_retries", c.retry.MaxRetries,
			"wait", wait.String(),
			"rate_limited", serverAsked,
			"error", cause.Error(),
		)
		if err := c.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
}

// permanentError marks a failure that happened before anything was sent, so
// repeating it cannot help.
type permanentError struct{ error }

func (c *Client) attempt(ctx context.Context, url string) (*Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, permanentError{fmt.Errorf("build request: %w", err)}
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if err := c.auth.Apply(req); err != nil {
		return nil, permanentError{fmt.Errorf("authenticate request: %w", err)}
	}

	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// The body is read here so the attempt's timeout covers it too.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	return &Response{Status: res.StatusCode, Header: res.Header, Body: body}, nil
}

// backoff returns the delay before retry number attempt: exponential growth
// capped at MaxDelay, with the upper half randomised so that clients which
// failed together do not retry together.
func (c *Client) backoff(attempt int) time.Duration {
	d := c.retry.BaseDelay
	for i := 1; i < attempt && d < c.retry.MaxDelay; i++ {
		d *= 2
	}
	if d > c.retry.MaxDelay {
		d = c.retry.MaxDelay
	}
	half := d / 2
	return half + time.Duration(c.rand()*float64(half))
}

// deferNextRequest records that the server's quota is spent, so the next
// request waits for it to refill instead of being sent and rejected.
func (c *Client) deferNextRequest(wait time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t := c.now().Add(wait); t.After(c.notBefore) {
		c.notBefore = t
	}
}

func (c *Client) waitUntilAllowed(ctx context.Context, log *slog.Logger) error {
	c.mu.Lock()
	wait := c.notBefore.Sub(c.now())
	c.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	if wait > c.retry.MaxWait {
		return fmt.Errorf("rate limit resets in %s, above the %s limit",
			wait.Round(time.Second), c.retry.MaxWait)
	}
	log.Warn("rate limit exhausted, waiting for reset", "wait", wait.String())
	return c.sleep(ctx, wait)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// StatusError is a non-2xx response.
type StatusError struct {
	Status int
	URL    string
	// Body is the start of the response body, for diagnosis.
	Body string
}

func newStatusError(url string, r *Response) *StatusError {
	const max = 200
	body := string(r.Body)
	if len(body) > max {
		body = body[:max] + "..."
	}
	return &StatusError{Status: r.Status, URL: url, Body: body}
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("GET %s: HTTP %d: %s", e.URL, e.Status, e.Body)
}

type loggerKey struct{}

// WithLogger attaches a logger to ctx. Requests made with that context log
// through it, so retries carry the caller's fields (repository, page).
func WithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, log)
}

// LoggerFrom returns the logger attached to ctx, or fallback.
func LoggerFrom(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if log, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return log
	}
	return fallback
}
