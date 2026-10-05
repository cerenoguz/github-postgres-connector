package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

const token = "ghp_supersecret"

func TestBearerSetsAuthorizationHeader(t *testing.T) {
	a, err := Bearer(NewSecret(token))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)

	if err := a.Apply(req); err != nil {
		t.Fatal(err)
	}

	if got, want := req.Header.Get("Authorization"), "Bearer "+token; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

func TestBasicSetsCredentials(t *testing.T) {
	a, err := Basic("me@example.test", NewSecret(token))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.test", nil)

	if err := a.Apply(req); err != nil {
		t.Fatal(err)
	}

	user, pass, ok := req.BasicAuth()
	if !ok || user != "me@example.test" || pass != token {
		t.Errorf("BasicAuth = %q, %q, %v", user, pass, ok)
	}
}

func TestEmptyCredentialsAreRejected(t *testing.T) {
	if _, err := Bearer(Secret{}); err == nil {
		t.Error("Bearer accepted an empty token")
	}
	if _, err := Basic("user", Secret{}); err == nil {
		t.Error("Basic accepted an empty password")
	}
}

func TestSecretNeverPrintsItsValue(t *testing.T) {
	s := NewSecret(token)
	holder := struct{ Token Secret }{s}

	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("msg", "token", s, "holder", holder)
	marshalled, err := json.Marshal(holder)
	if err != nil {
		t.Fatal(err)
	}

	outputs := map[string]string{
		"%v":   fmt.Sprintf("%v", s),
		"%+v":  fmt.Sprintf("%+v", holder),
		"%#v":  fmt.Sprintf("%#v", holder),
		"slog": logged.String(),
		"json": string(marshalled),
	}
	for name, out := range outputs {
		if strings.Contains(out, token) {
			t.Errorf("%s leaked the secret: %s", name, out)
		}
	}
}
