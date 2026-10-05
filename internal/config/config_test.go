package config

import (
	"strings"
	"testing"
	"time"
)

const full = `
database:
  url: postgres://app:${DB_PASSWORD}@localhost:5432/app
log:
  level: debug
  format: text
http:
  timeout: 10s
  retry:
    max_retries: 3
    base_delay: 250ms
    max_delay: 5s
    max_wait: 15m
connectors:
  - type: github
    auth:
      type: bearer
      token: ${GITHUB_TOKEN}
    repositories:
      - acme/widgets
      - acme/gadgets
    fetch_stats: true
`

func TestParseReadsEveryField(t *testing.T) {
	t.Setenv("DB_PASSWORD", "s3cret")
	t.Setenv("GITHUB_TOKEN", "ghp_token")

	cfg, err := Parse([]byte(full))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Database.URL != "postgres://app:s3cret@localhost:5432/app" {
		t.Errorf("database.url = %q", cfg.Database.URL)
	}
	if cfg.Log.Level != "debug" || cfg.Log.Format != "text" {
		t.Errorf("log = %+v", cfg.Log)
	}
	if time.Duration(cfg.HTTP.Timeout) != 10*time.Second {
		t.Errorf("http.timeout = %s", time.Duration(cfg.HTTP.Timeout))
	}
	r := cfg.HTTP.Retry
	if r.MaxRetries != 3 || time.Duration(r.BaseDelay) != 250*time.Millisecond ||
		time.Duration(r.MaxDelay) != 5*time.Second || time.Duration(r.MaxWait) != 15*time.Minute {
		t.Errorf("http.retry = %+v", r)
	}
	if len(cfg.Connectors) != 1 {
		t.Fatalf("connectors = %d, want 1", len(cfg.Connectors))
	}
	conn := cfg.Connectors[0]
	if conn.Type != "github" || conn.Auth.Type != "bearer" || conn.Auth.Token != "ghp_token" {
		t.Errorf("connector = %q, auth type %q", conn.Type, conn.Auth.Type)
	}
}

func TestConnectorSettingsAreDecodedByTheConnectorType(t *testing.T) {
	t.Setenv("DB_PASSWORD", "s3cret")
	t.Setenv("GITHUB_TOKEN", "ghp_token")
	cfg, err := Parse([]byte(full))
	if err != nil {
		t.Fatal(err)
	}

	var settings struct {
		Repositories []string `yaml:"repositories"`
		FetchStats   bool     `yaml:"fetch_stats"`
	}
	if err := cfg.Connectors[0].DecodeSettings(&settings); err != nil {
		t.Fatal(err)
	}

	if len(settings.Repositories) != 2 || settings.Repositories[1] != "acme/gadgets" || !settings.FetchStats {
		t.Errorf("settings = %+v", settings)
	}
}

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte("database:\n  url: postgres://localhost/app\nconnectors:\n  - type: github\n"))
	if err != nil {
		t.Fatal(err)
	}

	want := Defaults()
	if cfg.Log != want.Log || cfg.HTTP != want.HTTP {
		t.Errorf("log = %+v, http = %+v; want the defaults", cfg.Log, cfg.HTTP)
	}
}

func TestOnlyBracedReferencesAreExpanded(t *testing.T) {
	t.Setenv("word", "SHOULD-NOT-APPEAR")

	cfg, err := Parse([]byte("database:\n  url: postgres://app:pa$word@localhost/app\nconnectors:\n  - type: github\n"))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Database.URL != "postgres://app:pa$word@localhost/app" {
		t.Errorf("database.url = %q, want the $ left alone", cfg.Database.URL)
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	const valid = "database:\n  url: postgres://localhost/app\nconnectors:\n  - type: github\n"
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{"unset variable", "database:\n  url: ${NOT_SET_ANYWHERE}\nconnectors:\n  - type: github\n", "environment variable NOT_SET_ANYWHERE is not set"},
		{"unknown key", valid + "databse: {}\n", "databse"},
		{"missing database url", "connectors:\n  - type: github\n", "database.url is required"},
		{"no connectors", "database:\n  url: postgres://localhost/app\n", "at least one connector"},
		{"connector without type", "database:\n  url: postgres://localhost/app\nconnectors:\n  - repositories: [a/b]\n", "connectors[0].type is required"},
		{"bad duration", valid + "http:\n  timeout: soon\n", "is not a duration"},
		{"bad log level", valid + "log:\n  level: loud\n", "log.level"},
		{"negative retries", valid + "http:\n  retry:\n    max_retries: -1\n", "max_retries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestErrorsDoNotEchoSecrets(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_supersecret")

	_, err := Parse([]byte("database:\n  url: postgres://localhost/app\nconnectors:\n  - type: github\n    auth:\n      token: ${GITHUB_TOKEN}\nlog:\n  level: loud\n"))

	if err == nil || strings.Contains(err.Error(), "ghp_supersecret") {
		t.Errorf("err = %v", err)
	}
}
