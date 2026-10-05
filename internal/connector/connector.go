// Package connector defines the contract between the sync engine and a
// source-specific connector (GitHub, GitLab, Jira, ...).
//
// A connector knows how to talk to one tool and how to map its payloads into
// the normalized types below. It knows nothing about the database, about
// retries, or about how credentials are attached to requests.
package connector

import (
	"context"
	"time"
)

// Cursor is an opaque position in a resource's history. Its meaning belongs
// to the connector that produced it (a timestamp for GitHub commits, possibly
// a page token elsewhere); the engine only stores it and hands it back.
// The zero value means "start from the beginning".
type Cursor string

// Person is a commit author or committer. Login is the account on the source
// tool and is empty when the commit email is not linked to an account.
type Person struct {
	Name  string
	Email string
	Login string
}

// Commit is the normalized commit shared by every source that has commits.
type Commit struct {
	SHA         string
	Repository  string
	Message     string
	Author      Person
	Committer   Person
	AuthoredAt  time.Time
	CommittedAt time.Time
	URL         string
	ParentCount int
	// Additions and Deletions are nil when stats were not fetched.
	Additions *int
	Deletions *int
}

// Batch is one page of records. New entity kinds (issues, pull requests) are
// added as new fields here, so existing connectors keep compiling untouched.
type Batch struct {
	Page    int
	Commits []Commit
}

// Len is the number of records in the batch, of every kind. The engine
// counts with it, so it never needs to know which kinds exist.
func (b Batch) Len() int {
	return len(b.Commits)
}

// EmitFunc hands a batch to the engine for persistence. A connector must stop
// and return the error if it fails.
type EmitFunc func(ctx context.Context, b Batch) error

// Connector extracts data from one source tool.
type Connector interface {
	// Name identifies the source, e.g. "github". It namespaces rows and cursors.
	Name() string

	// Resources lists the independently synced units, e.g. "owner/repo".
	Resources() []string

	// Fetch emits every record of resource newer than since, page by page,
	// and returns the cursor to resume from next time. The returned cursor is
	// only meaningful when err is nil; the engine discards it otherwise.
	Fetch(ctx context.Context, resource string, since Cursor, emit EmitFunc) (Cursor, error)
}
