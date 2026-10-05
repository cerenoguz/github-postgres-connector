package github

import (
	"net/http"
	"strconv"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

// resetPadding is added to a rate-limit reset time, which GitHub reports in
// whole seconds, so that we do not wake up a moment before the quota refills.
const resetPadding = time.Second

// Classifier teaches the shared HTTP client GitHub's rate-limit rules.
//
//   - Primary limit: 403 or 429 with X-RateLimit-Remaining: 0. Wait until the
//     time in X-RateLimit-Reset.
//   - Secondary limit: 403 or 429 with Retry-After. Wait that long.
//   - A successful response that used the last request of the quota delays
//     the next request until the reset, so the limit is never actually hit.
//   - Any other 403 is a real permission error and is not retried.
//
// Everything else falls through to plain HTTP semantics.
func Classifier(status int, h http.Header, now time.Time) httpx.Decision {
	exhausted := h.Get("X-RateLimit-Remaining") == "0"

	switch {
	case status >= 200 && status < 300:
		if exhausted {
			return httpx.Decision{Wait: untilReset(h, now)}
		}
		return httpx.Decision{}

	case status == http.StatusForbidden || status == http.StatusTooManyRequests:
		if wait, ok := httpx.RetryAfter(h, now); ok {
			return httpx.Decision{Retry: true, Wait: wait}
		}
		if exhausted {
			return httpx.Decision{Retry: true, Wait: untilReset(h, now)}
		}
	}
	return httpx.DefaultClassifier(status, h, now)
}

// untilReset returns how long until the quota refills according to
// X-RateLimit-Reset (Unix seconds). It returns 0, meaning "use the normal
// backoff", when the header is missing or unreadable.
func untilReset(h http.Header, now time.Time) time.Duration {
	secs, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return 0
	}
	wait := time.Unix(secs, 0).Sub(now) + resetPadding
	if wait < resetPadding {
		return resetPadding
	}
	return wait
}
