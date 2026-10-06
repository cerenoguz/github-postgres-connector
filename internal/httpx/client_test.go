package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
)

var testPolicy = RetryPolicy{
	MaxRetries: 3,
	BaseDelay:  100 * time.Millisecond,
	MaxDelay:   time.Second,
	MaxWait:    time.Minute,
}

// testClient returns a client whose sleeps are recorded instead of slept and
// whose jitter is fixed at its maximum.
func testClient(o Options) (*Client, *[]time.Duration) {
	if o.Retry == (RetryPolicy{}) {
		o.Retry = testPolicy
	}
	c := New(o)
	var sleeps []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	c.rand = func() float64 { return 1 }
	return c, &sleeps
}

// sequence serves the given handlers in order, repeating the last one.
func sequence(t *testing.T, handlers ...http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(handlers) {
			i = len(handlers) - 1
		}
		handlers[i](w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

func ok(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(body)) }
}

func TestGetRetriesTransientStatusesWithExponentialBackoff(t *testing.T) {
	srv, calls := sequence(t, status(500), status(502), status(503), ok("done"))
	c, sleeps := testClient(Options{})

	resp, err := c.Get(context.Background(), srv.URL)

	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "done" {
		t.Errorf("body = %q", resp.Body)
	}
	if calls.Load() != 4 {
		t.Errorf("calls = %d, want 4", calls.Load())
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	if len(*sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", *sleeps, want)
	}
	for i := range want {
		if (*sleeps)[i] != want[i] {
			t.Errorf("sleep %d = %s, want %s", i, (*sleeps)[i], want[i])
		}
	}
}

func TestGetDoesNotRetryPermanentStatuses(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 422} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv, calls := sequence(t, status(code))
			c, sleeps := testClient(Options{})

			_, err := c.Get(context.Background(), srv.URL)

			var se *StatusError
			if !errors.As(err, &se) || se.Status != code {
				t.Fatalf("err = %v, want StatusError %d", err, code)
			}
			if calls.Load() != 1 || len(*sleeps) != 0 {
				t.Errorf("calls = %d, sleeps = %v; want a single attempt", calls.Load(), *sleeps)
			}
		})
	}
}

func TestGetGivesUpAfterMaxRetries(t *testing.T) {
	srv, calls := sequence(t, status(503))
	c, _ := testClient(Options{})

	_, err := c.Get(context.Background(), srv.URL)

	var se *StatusError
	if !errors.As(err, &se) || se.Status != 503 {
		t.Fatalf("err = %v, want it to wrap the last StatusError", err)
	}
	if !strings.Contains(err.Error(), "giving up after 4 attempts") {
		t.Errorf("err = %v, want it to say how many attempts were made", err)
	}
	if calls.Load() != 4 {
		t.Errorf("calls = %d, want 1 attempt + 3 retries", calls.Load())
	}
}

func TestGetHonoursRetryAfterOn429(t *testing.T) {
	srv, _ := sequence(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
		},
		ok("done"),
	)
	c, sleeps := testClient(Options{})

	if _, err := c.Get(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}

	if len(*sleeps) != 1 || (*sleeps)[0] != 7*time.Second {
		t.Errorf("sleeps = %v, want exactly the 7s the server asked for", *sleeps)
	}
}

func TestGetFailsWhenServerWaitExceedsMaxWait(t *testing.T) {
	srv, calls := sequence(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
	})
	c, sleeps := testClient(Options{})

	_, err := c.Get(context.Background(), srv.URL)

	if err == nil || !strings.Contains(err.Error(), "above the 1m0s limit") {
		t.Fatalf("err = %v, want the wait to be refused", err)
	}
	if calls.Load() != 1 || len(*sleeps) != 0 {
		t.Errorf("calls = %d, sleeps = %v; want no waiting", calls.Load(), *sleeps)
	}
}

func TestGetRetriesNetworkErrors(t *testing.T) {
	srv, calls := sequence(t,
		func(w http.ResponseWriter, _ *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close() // drop the connection without answering
		},
		ok("done"),
	)
	c, sleeps := testClient(Options{})

	resp, err := c.Get(context.Background(), srv.URL)

	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "done" || calls.Load() != 2 || len(*sleeps) != 1 {
		t.Errorf("body = %q, calls = %d, sleeps = %v", resp.Body, calls.Load(), *sleeps)
	}
}

func TestGetRetriesAttemptTimeout(t *testing.T) {
	srv, calls := sequence(t,
		func(_ http.ResponseWriter, r *http.Request) {
			select { // slower than the client's timeout
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		},
		ok("done"),
	)
	c, _ := testClient(Options{Timeout: 50 * time.Millisecond})

	resp, err := c.Get(context.Background(), srv.URL)

	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "done" || calls.Load() != 2 {
		t.Errorf("body = %q, calls = %d", resp.Body, calls.Load())
	}
}

func TestGetStopsWhenContextIsCancelledDuringBackoff(t *testing.T) {
	srv, calls := sequence(t, status(503))
	c := New(Options{Retry: RetryPolicy{MaxRetries: 5, BaseDelay: time.Hour, MaxDelay: time.Hour}})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	_, err := c.Get(ctx, srv.URL)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want no retry after cancellation", calls.Load())
	}
}

func TestGetDoesNotRetryWhenContextIsAlreadyCancelled(t *testing.T) {
	srv, calls := sequence(t, ok("done"))
	c, sleeps := testClient(Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.Get(ctx, srv.URL)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 || len(*sleeps) != 0 {
		t.Errorf("calls = %d, sleeps = %v", calls.Load(), *sleeps)
	}
}

func TestGetSendsCredentialsAndHeadersOnEveryAttempt(t *testing.T) {
	const token = "ghp_supersecret"
	var seen []string
	record := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			seen = append(seen, r.Header.Get("Authorization")+"|"+r.Header.Get("Accept"))
			next(w, r)
		}
	}
	srv, _ := sequence(t, record(status(500)), record(ok("done")))
	bearer, _ := auth.Bearer(auth.NewSecret(token))
	c, _ := testClient(Options{Auth: bearer, Headers: map[string]string{"Accept": "application/json"}})

	if _, err := c.Get(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}

	want := "Bearer " + token + "|application/json"
	if len(seen) != 2 || seen[0] != want || seen[1] != want {
		t.Errorf("seen = %v, want %q twice", seen, want)
	}
}

func TestErrorsNeverContainTheToken(t *testing.T) {
	const token = "ghp_supersecret"
	bearer, _ := auth.Bearer(auth.NewSecret(token))

	t.Run("status error", func(t *testing.T) {
		srv, _ := sequence(t, status(401))
		c, _ := testClient(Options{Auth: bearer})
		_, err := c.Get(context.Background(), srv.URL)
		if err == nil || strings.Contains(err.Error(), token) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("network error", func(t *testing.T) {
		srv, _ := sequence(t, ok(""))
		srv.Close() // nothing is listening any more
		c, _ := testClient(Options{Auth: bearer})
		_, err := c.Get(context.Background(), srv.URL)
		if err == nil || strings.Contains(err.Error(), token) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestSuccessWithWaitDelaysTheNextRequest(t *testing.T) {
	srv, _ := sequence(t, ok("one"), ok("two"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A quota that is spent by the first response and refills in 30 seconds.
	spent := true
	classify := func(int, http.Header, time.Time) Decision {
		if spent {
			spent = false
			return Decision{Wait: 30 * time.Second}
		}
		return Decision{}
	}
	c, sleeps := testClient(Options{Classify: classify})
	c.now = func() time.Time { return now }

	if _, err := c.Get(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	if len(*sleeps) != 0 {
		t.Fatalf("slept %v before the quota was known to be spent", *sleeps)
	}
	if _, err := c.Get(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}

	if len(*sleeps) != 1 || (*sleeps)[0] != 30*time.Second {
		t.Errorf("sleeps = %v, want one 30s wait before the second request", *sleeps)
	}
}

func TestBackoffIsJitteredAndCapped(t *testing.T) {
	c := New(Options{Retry: testPolicy})

	c.rand = func() float64 { return 0 }
	if got := c.backoff(1); got != 50*time.Millisecond {
		t.Errorf("minimum first backoff = %s, want half the base delay", got)
	}
	c.rand = func() float64 { return 1 }
	if got := c.backoff(1); got != 100*time.Millisecond {
		t.Errorf("maximum first backoff = %s, want the base delay", got)
	}
	if got := c.backoff(50); got != time.Second {
		t.Errorf("backoff(50) = %s, want it capped at MaxDelay", got)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
	}{
		{"absent", "", 0, false},
		{"seconds", "120", 2 * time.Minute, true},
		{"http date", "Thu, 01 Jan 2026 12:00:30 GMT", 30 * time.Second, true},
		{"date in the past", "Thu, 01 Jan 2026 11:00:00 GMT", 0, true},
		{"negative", "-5", 0, false},
		{"garbage", "soon", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			if tt.value != "" {
				h.Set("Retry-After", tt.value)
			}
			got, ok := RetryAfter(h, now)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("RetryAfter = %s, %v; want %s, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestGetRejectsOversizedBodyWithoutRetrying(t *testing.T) {
	srv, calls := sequence(t, ok(strings.Repeat("x", 11)))
	c, sleeps := testClient(Options{MaxBodyBytes: 10})

	_, err := c.Get(context.Background(), srv.URL)

	if err == nil || !strings.Contains(err.Error(), "exceeds the 10 byte limit") {
		t.Fatalf("err = %v, want the body to be refused", err)
	}
	if calls.Load() != 1 || len(*sleeps) != 0 {
		t.Errorf("calls = %d, sleeps = %v; want a single attempt", calls.Load(), *sleeps)
	}
}

func TestGetAcceptsBodyExactlyAtTheLimit(t *testing.T) {
	srv, _ := sequence(t, ok(strings.Repeat("x", 10)))
	c, _ := testClient(Options{MaxBodyBytes: 10})

	resp, err := c.Get(context.Background(), srv.URL)

	if err != nil || len(resp.Body) != 10 {
		t.Errorf("err = %v, want a 10 byte body accepted", err)
	}
}

func TestStatusErrorBodyIsFlattenedToOnePrintableLine(t *testing.T) {
	srv, _ := sequence(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte("{\n  \"message\": \"Bad credentials\",\x1b[31m\r\n  \"status\": \"401\"\n}"))
	})
	c, _ := testClient(Options{})

	_, err := c.Get(context.Background(), srv.URL)

	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v, want a StatusError", err)
	}
	if want := `{ "message": "Bad credentials", [31m "status": "401" }`; se.Body != want {
		t.Errorf("body = %q, want %q", se.Body, want)
	}
}
