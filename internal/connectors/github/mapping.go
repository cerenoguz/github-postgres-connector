package github

import (
	"strings"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
)

// apiCommit is the part of GitHub's commit object that we use. The same
// shape is returned by the list, compare and single-commit endpoints; only
// the last one fills Stats.
type apiCommit struct {
	SHA     string `json:"sha"`
	HTMLURL string `json:"html_url"`
	Commit  struct {
		Message   string       `json:"message"`
		Author    apiSignature `json:"author"`
		Committer apiSignature `json:"committer"`
	} `json:"commit"`
	// Author and Committer are the GitHub accounts matched to the commit
	// emails. They are null when an email is not linked to any account.
	Author    *apiUser `json:"author"`
	Committer *apiUser `json:"committer"`
	Parents   []struct {
		SHA string `json:"sha"`
	} `json:"parents"`
	Stats *struct {
		Additions int `json:"additions"`
		Deletions int `json:"deletions"`
	} `json:"stats"`
}

// apiSignature is the name, email and date recorded in the git commit itself.
type apiSignature struct {
	Name  string    `json:"name"`
	Email string    `json:"email"`
	Date  time.Time `json:"date"`
}

type apiUser struct {
	Login string `json:"login"`
}

// toCommit maps a GitHub commit to the normalized model.
func toCommit(repository string, c apiCommit) connector.Commit {
	out := connector.Commit{
		SHA:        c.SHA,
		Repository: repository,
		Message:    clean(c.Commit.Message),
		Author: connector.Person{
			Name:  clean(c.Commit.Author.Name),
			Email: clean(c.Commit.Author.Email),
			Login: login(c.Author),
		},
		Committer: connector.Person{
			Name:  clean(c.Commit.Committer.Name),
			Email: clean(c.Commit.Committer.Email),
			Login: login(c.Committer),
		},
		AuthoredAt:  c.Commit.Author.Date.UTC(),
		CommittedAt: c.Commit.Committer.Date.UTC(),
		URL:         c.HTMLURL,
		ParentCount: len(c.Parents),
	}
	if c.Stats != nil {
		out.Additions = &c.Stats.Additions
		out.Deletions = &c.Stats.Deletions
	}
	return out
}

func login(u *apiUser) string {
	if u == nil {
		return ""
	}
	return u.Login
}

// clean removes NUL bytes, which JSON can carry but PostgreSQL text cannot
// store. Without this, one odd commit would fail its repository on every run.
func clean(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}
