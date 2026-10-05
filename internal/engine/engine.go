// Package engine runs connectors and owns the consistency rules of a sync:
// batches are persisted as they arrive, a resource's cursor advances only
// after all of its batches are stored, and one resource failing does not stop
// the others.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
)

// Store is what the engine needs from persistence. SaveBatch must be atomic
// and idempotent: a batch is stored entirely or not at all, and storing the
// same records twice inserts nothing the second time.
type Store interface {
	Cursor(ctx context.Context, source, resource string) (connector.Cursor, error)
	SaveBatch(ctx context.Context, source string, b connector.Batch) (inserted int, err error)
	SaveCursor(ctx context.Context, source, resource string, c connector.Cursor) error
}

// Result is the outcome of syncing one resource.
type Result struct {
	Source   string
	Resource string
	Read     int
	Inserted int
	Duration time.Duration
	Err      error
}

// Report is the outcome of a whole run.
type Report struct {
	Results []Result
}

// Failed reports whether any resource failed.
func (r Report) Failed() bool {
	for _, res := range r.Results {
		if res.Err != nil {
			return true
		}
	}
	return false
}

type Engine struct {
	store Store
	log   *slog.Logger
	// Full ignores stored cursors and re-reads the whole history. Rows that
	// already exist are left alone, so it is safe to use for repair.
	Full bool
}

func New(store Store, log *slog.Logger) *Engine {
	return &Engine{store: store, log: log}
}

// Sync runs every resource of every connector in order. It never returns
// early on a resource error; after cancellation the remaining resources are
// reported as failed with the context's error.
func (e *Engine) Sync(ctx context.Context, conns []connector.Connector) Report {
	var report Report
	for _, c := range conns {
		for _, resource := range c.Resources() {
			res := Result{Source: c.Name(), Resource: resource}
			if err := ctx.Err(); err != nil {
				res.Err = err
			} else {
				res = e.syncResource(ctx, c, resource)
			}
			report.Results = append(report.Results, res)
		}
	}
	return report
}

func (e *Engine) syncResource(ctx context.Context, c connector.Connector, resource string) (res Result) {
	res = Result{Source: c.Name(), Resource: resource}
	log := e.log.With("connector", c.Name(), "resource", resource)
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()

	var since connector.Cursor
	if !e.Full {
		var err error
		if since, err = e.store.Cursor(ctx, c.Name(), resource); err != nil {
			res.Err = fmt.Errorf("load cursor: %w", err)
			log.Error("sync failed", "error", res.Err)
			return res
		}
	}
	log.Info("sync started", "cursor", string(since), "full", e.Full)

	emit := func(ctx context.Context, b connector.Batch) error {
		inserted, err := e.store.SaveBatch(ctx, c.Name(), b)
		if err != nil {
			return fmt.Errorf("save page %d: %w", b.Page, err)
		}
		res.Read += b.Len()
		res.Inserted += inserted
		log.Info("page stored", "page", b.Page, "read", b.Len(), "inserted", inserted)
		return nil
	}

	next, err := c.Fetch(ctx, resource, since, emit)
	if err != nil {
		// The cursor stays where it was: pages already stored are skipped as
		// duplicates on the next run, and nothing after them is lost.
		res.Err = err
		log.Error("sync failed", "error", err, "read", res.Read, "inserted", res.Inserted)
		return res
	}

	if next != since {
		if err := e.store.SaveCursor(ctx, c.Name(), resource, next); err != nil {
			res.Err = fmt.Errorf("save cursor: %w", err)
			log.Error("sync failed", "error", res.Err)
			return res
		}
	}
	log.Info("sync finished", "read", res.Read, "inserted", res.Inserted, "cursor", string(next))
	return res
}
