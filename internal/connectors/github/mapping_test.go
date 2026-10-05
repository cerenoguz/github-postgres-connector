package github

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
)

func loadFixture(t *testing.T) apiCommit {
	t.Helper()
	raw, err := os.ReadFile("testdata/commit.json")
	if err != nil {
		t.Fatal(err)
	}
	var c apiCommit
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestToCommitMapsEveryField(t *testing.T) {
	got := toCommit("acme/widgets", loadFixture(t))

	want := connector.Commit{
		SHA:        "7f05d217867b2af52b0a28c6d1c91df97e1b5b39",
		Repository: "acme/widgets",
		Message:    "Merge pull request #42 from acme/feature\n\nAdd the analytical engine",
		Author:     connector.Person{Name: "Ada Lovelace", Email: "ada@example.test", Login: "ada"},
		// The committer email is not linked to an account, so there is no login.
		Committer:   connector.Person{Name: "GitHub", Email: "noreply@github.com"},
		AuthoredAt:  time.Date(2026, 3, 1, 8, 15, 30, 0, time.UTC),
		CommittedAt: time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC),
		URL:         "https://github.com/acme/widgets/commit/7f05d217867b2af52b0a28c6d1c91df97e1b5b39",
		ParentCount: 2,
	}

	if got.Additions == nil || *got.Additions != 45 || got.Deletions == nil || *got.Deletions != 12 {
		t.Errorf("stats = %v, %v; want 45, 12", got.Additions, got.Deletions)
	}
	got.Additions, got.Deletions = nil, nil
	if got != want {
		t.Errorf("toCommit =\n%+v\nwant\n%+v", got, want)
	}
}

func TestToCommitNormalisesDatesToUTC(t *testing.T) {
	got := toCommit("acme/widgets", loadFixture(t))

	// The fixture's author date carries a +02:00 offset.
	if got.AuthoredAt.Location() != time.UTC {
		t.Errorf("AuthoredAt location = %s, want UTC", got.AuthoredAt.Location())
	}
}

func TestToCommitWithoutStatsLeavesThemNil(t *testing.T) {
	c := loadFixture(t)
	c.Stats = nil

	got := toCommit("acme/widgets", c)

	if got.Additions != nil || got.Deletions != nil {
		t.Errorf("stats = %v, %v; want nil", got.Additions, got.Deletions)
	}
}

func TestToCommitCountsParents(t *testing.T) {
	c := loadFixture(t)
	c.Parents = nil // a root commit

	if got := toCommit("acme/widgets", c).ParentCount; got != 0 {
		t.Errorf("ParentCount = %d, want 0 for a root commit", got)
	}
}

func TestToCommitStripsNULBytes(t *testing.T) {
	var c apiCommit
	if err := json.Unmarshal([]byte(`{"commit":{"message":"a\u0000b","author":{"name":"n\u0000"}}}`), &c); err != nil {
		t.Fatal(err)
	}

	got := toCommit("acme/widgets", c)

	if got.Message != "ab" || got.Author.Name != "n" {
		t.Errorf("message = %q, author = %q; want NUL bytes removed", got.Message, got.Author.Name)
	}
}
