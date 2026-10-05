package app

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
	"github.com/cerenoguz/github-postgres-connector/internal/config"
	"github.com/cerenoguz/github-postgres-connector/internal/connector"
	"github.com/cerenoguz/github-postgres-connector/internal/connectors/github"
	"github.com/cerenoguz/github-postgres-connector/internal/httpx"
)

// factory builds one connector type from its entry in the config file.
// newClient returns the shared HTTP client configured with the entry's
// credentials plus whatever is specific to the tool's API.
type factory func(spec config.ConnectorSpec, newClient clientBuilder, log *slog.Logger) (connector.Connector, error)

type clientBuilder func(classify httpx.Classifier, headers map[string]string) *httpx.Client

// factories is the list of connector types this binary knows. Adding GitLab
// or Jira means adding a package under internal/connectors and one line here.
var factories = map[string]factory{
	github.Name: newGitHub,
}

func buildConnectors(cfg config.Config, log *slog.Logger) ([]connector.Connector, error) {
	var conns []connector.Connector
	for i, spec := range cfg.Connectors {
		build, ok := factories[spec.Type]
		if !ok {
			known := make([]string, 0, len(factories))
			for name := range factories {
				known = append(known, name)
			}
			slices.Sort(known)
			return nil, fmt.Errorf("connectors[%d]: unknown type %q (known: %s)", i, spec.Type, strings.Join(known, ", "))
		}
		authn, err := buildAuth(spec.Auth)
		if err != nil {
			return nil, fmt.Errorf("connectors[%d] (%s): %w", i, spec.Type, err)
		}
		newClient := func(classify httpx.Classifier, headers map[string]string) *httpx.Client {
			return httpx.New(httpx.Options{
				Auth:     authn,
				Timeout:  time.Duration(cfg.HTTP.Timeout),
				Classify: classify,
				Headers:  headers,
				Logger:   log,
				Retry: httpx.RetryPolicy{
					MaxRetries: cfg.HTTP.Retry.MaxRetries,
					BaseDelay:  time.Duration(cfg.HTTP.Retry.BaseDelay),
					MaxDelay:   time.Duration(cfg.HTTP.Retry.MaxDelay),
					MaxWait:    time.Duration(cfg.HTTP.Retry.MaxWait),
				},
			})
		}
		conn, err := build(spec, newClient, log)
		if err != nil {
			return nil, fmt.Errorf("connectors[%d]: %w", i, err)
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

// buildAuth turns the auth section into an Authenticator. This is the only
// place that maps scheme names to implementations.
func buildAuth(a config.Auth) (auth.Authenticator, error) {
	kind := a.Type
	if kind == "" {
		kind = "none"
		if a.Token != "" {
			kind = "bearer"
		}
	}
	switch kind {
	case "bearer":
		return auth.Bearer(auth.NewSecret(string(a.Token)))
	case "basic":
		return auth.Basic(string(a.Username), auth.NewSecret(string(a.Password)))
	case "none":
		return auth.None(), nil
	}
	return nil, fmt.Errorf("unknown auth type %q (known: bearer, basic, none)", a.Type)
}

func newGitHub(spec config.ConnectorSpec, newClient clientBuilder, log *slog.Logger) (connector.Connector, error) {
	var settings struct {
		BaseURL      string   `yaml:"base_url"`
		Repositories []string `yaml:"repositories"`
		PerPage      int      `yaml:"per_page"`
		FetchStats   bool     `yaml:"fetch_stats"`
	}
	if err := spec.DecodeSettings(&settings); err != nil {
		return nil, fmt.Errorf("github: %w", err)
	}
	return github.New(github.Config{
		BaseURL:      settings.BaseURL,
		Repositories: settings.Repositories,
		PerPage:      settings.PerPage,
		FetchStats:   settings.FetchStats,
	}, newClient(github.Classifier, github.Headers), log)
}
