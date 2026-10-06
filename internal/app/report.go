package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/cerenoguz/github-postgres-connector/internal/engine"
)

// writeReport prints the per-resource summary of a run: what was read from
// the source, how much of it was new, and why anything failed.
func writeReport(w io.Writer, report engine.Report) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CONNECTOR\tRESOURCE\tSTATUS\tREAD\tINSERTED\tDURATION")

	var read, inserted, failed int
	for _, r := range report.Results {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\n",
			r.Source, r.Resource, status(r), r.Read, r.Inserted, r.Duration.Round(time.Millisecond))
		read += r.Read
		inserted += r.Inserted
		if r.Err != nil {
			failed++
		}
	}
	tw.Flush()

	fmt.Fprintf(w, "\n%s, %d failed, %s read, %d inserted\n",
		count(len(report.Results), "resource"), failed, count(read, "record"), inserted)

	if failed > 0 {
		fmt.Fprintln(w, "\nErrors:")
		for _, r := range report.Results {
			if r.Err != nil {
				fmt.Fprintf(w, "  %s %s: %v\n", r.Source, r.Resource, r.Err)
			}
		}
	}
}

// count formats n with a noun, pluralised unless n is exactly one.
func count(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func status(r engine.Result) string {
	switch {
	case r.Err == nil:
		return "ok"
	case errors.Is(r.Err, context.Canceled):
		return "cancelled"
	default:
		return "failed"
	}
}
