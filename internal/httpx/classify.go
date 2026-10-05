package httpx

import (
	"net/http"
	"strconv"
	"time"
)

// Decision is what the client should do about a response.
type Decision struct {
	// Retry says a non-2xx response is transient and worth another attempt.
	Retry bool
	// Wait is how long the server asked us to hold off. With Retry it replaces
	// the computed backoff. On a 2xx response it delays the next request,
	// which is how an exhausted quota is waited out before it causes an error.
	Wait time.Duration
}

// Classifier interprets a response for one API. now is passed in so that
// waits derived from absolute times are testable.
type Classifier func(status int, h http.Header, now time.Time) Decision

// DefaultClassifier applies plain HTTP semantics: 429 and the transient 5xx
// codes are retried, honouring Retry-After when present, and every other
// non-2xx status is permanent.
func DefaultClassifier(status int, h http.Header, now time.Time) Decision {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		wait, _ := RetryAfter(h, now)
		return Decision{Retry: true, Wait: wait}
	}
	return Decision{}
}

// RetryAfter reads the Retry-After header, which is either a number of
// seconds or an HTTP date.
func RetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
		return 0, true
	}
	return 0, false
}
