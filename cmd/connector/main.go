// Command connector syncs engineering-tool activity into PostgreSQL.
//
//	connector sync --config config.yaml
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cerenoguz/github-postgres-connector/internal/app"
)

func main() {
	// The first Ctrl+C (or SIGTERM) cancels the context: the request in
	// flight is aborted, the open transaction rolls back, and the run ends
	// with a report. Cursors of unfinished repositories are left untouched.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Once cancelled, stop intercepting so a second Ctrl+C kills the process.
	go func() {
		<-ctx.Done()
		stop()
	}()

	os.Exit(app.Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
