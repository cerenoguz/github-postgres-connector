// Package auth attaches credentials to outgoing requests. The HTTP client
// calls an Authenticator on every attempt, so extraction code never handles a
// credential, and a new scheme (GitHub App, OAuth) is a new Authenticator.
package auth

import (
	"errors"
	"log/slog"
	"net/http"
)

// Authenticator adds credentials to a request. Apply runs on every attempt,
// which lets a future implementation refresh an expiring token.
type Authenticator interface {
	Apply(req *http.Request) error
}

const redacted = "[REDACTED]"

// Secret holds a credential and refuses to print it: formatting it with any
// fmt verb, logging it with slog or marshalling it yields "[REDACTED]".
type Secret struct {
	value string
}

func NewSecret(v string) Secret { return Secret{value: v} }

// Reveal returns the credential. It is the only way to read it.
func (s Secret) Reveal() string { return s.value }

func (s Secret) IsZero() bool                 { return s.value == "" }
func (s Secret) String() string               { return redacted }
func (s Secret) GoString() string             { return redacted }
func (s Secret) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (s Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }

type bearer struct{ token Secret }

// Bearer sends "Authorization: Bearer <token>", the scheme GitHub expects for
// personal access tokens.
func Bearer(token Secret) (Authenticator, error) {
	if token.IsZero() {
		return nil, errors.New("auth: bearer token is empty")
	}
	return bearer{token}, nil
}

func (b bearer) Apply(req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+b.token.Reveal())
	return nil
}

type basic struct {
	user     string
	password Secret
}

// Basic sends HTTP Basic credentials, the scheme Jira uses with API tokens.
func Basic(user string, password Secret) (Authenticator, error) {
	if user == "" || password.IsZero() {
		return nil, errors.New("auth: basic credentials are incomplete")
	}
	return basic{user, password}, nil
}

func (b basic) Apply(req *http.Request) error {
	req.SetBasicAuth(b.user, b.password.Reveal())
	return nil
}

type none struct{}

// None sends no credentials.
func None() Authenticator { return none{} }

func (none) Apply(*http.Request) error { return nil }
