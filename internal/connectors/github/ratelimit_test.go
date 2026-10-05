package github

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

func TestClassifier(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	resetIn := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }

	tests := []struct {
		name   string
		status int
		header map[string]string
		want   httpx.Decision
	}{
		{
			name:   "ok with quota left",
			status: 200,
			header: map[string]string{"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": resetIn(time.Hour)},
			want:   httpx.Decision{},
		},
		{
			name:   "ok that spent the last request delays the next one",
			status: 200,
			header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": resetIn(10 * time.Minute)},
			want:   httpx.Decision{Wait: 10*time.Minute + resetPadding},
		},
		{
			name:   "primary rate limit as 403 waits for the reset",
			status: 403,
			header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": resetIn(90 * time.Second)},
			want:   httpx.Decision{Retry: true, Wait: 90*time.Second + resetPadding},
		},
		{
			name:   "primary rate limit as 429 waits for the reset",
			status: 429,
			header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": resetIn(90 * time.Second)},
			want:   httpx.Decision{Retry: true, Wait: 90*time.Second + resetPadding},
		},
		{
			name:   "reset already in the past still waits a moment",
			status: 403,
			header: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": resetIn(-time.Minute)},
			want:   httpx.Decision{Retry: true, Wait: resetPadding},
		},
		{
			name:   "exhausted without a readable reset falls back to backoff",
			status: 403,
			header: map[string]string{"X-RateLimit-Remaining": "0"},
			want:   httpx.Decision{Retry: true},
		},
		{
			name:   "secondary rate limit honours Retry-After",
			status: 403,
			header: map[string]string{"Retry-After": "60", "X-RateLimit-Remaining": "4000"},
			want:   httpx.Decision{Retry: true, Wait: time.Minute},
		},
		{
			name:   "Retry-After wins over the reset time",
			status: 429,
			header: map[string]string{"Retry-After": "5", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": resetIn(time.Hour)},
			want:   httpx.Decision{Retry: true, Wait: 5 * time.Second},
		},
		{
			name:   "403 without rate limiting is a permission error",
			status: 403,
			header: map[string]string{"X-RateLimit-Remaining": "4000"},
			want:   httpx.Decision{},
		},
		{name: "401 is permanent", status: 401, want: httpx.Decision{}},
		{name: "404 is permanent", status: 404, want: httpx.Decision{}},
		{name: "422 is permanent", status: 422, want: httpx.Decision{}},
		{name: "429 without hints uses backoff", status: 429, want: httpx.Decision{Retry: true}},
		{name: "502 is transient", status: 502, want: httpx.Decision{Retry: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tt.header {
				h.Set(k, v)
			}
			if got := Classifier(tt.status, h, now); got != tt.want {
				t.Errorf("Classifier = %+v, want %+v", got, tt.want)
			}
		})
	}
}
