// Package app is the command-line application: it reads the config, wires
// the connectors, the store and the engine together, and reports the result.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/cerenoguz/github-postgres-connector/internal/config"
	"github.com/cerenoguz/github-postgres-connector/internal/engine"
	"github.com/cerenoguz/github-postgres-connector/internal/store"
)

// Exit codes.
const (
	ExitOK = 0
	// ExitSyncFailed means the run happened but at least one resource (for
	// GitHub, a repository) failed or was cancelled.
	ExitSyncFailed = 1
	// ExitSetup means the run could not start: bad flags, bad config, or no
	// database.
	ExitSetup = 2
)

var errSyncFailed = errors.New("one or more resources failed")

var _ engine.Store = (*store.Store)(nil)

// Run executes the CLI and returns the process exit code. The report goes to
// stdout and logs and errors to stderr, so the two can be redirected apart.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	root := &cobra.Command{
		Use:           "connector",
		Short:         "Sync engineering-tool activity into PostgreSQL",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.AddCommand(syncCommand(stdout, stderr), migrateCommand(stdout))

	err := root.ExecuteContext(ctx)
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, errSyncFailed):
		return ExitSyncFailed
	default:
		fmt.Fprintln(stderr, "error:", err)
		return ExitSetup
	}
}

func syncCommand(stdout, stderr io.Writer) *cobra.Command {
	var configPath string
	var full, skipMigrations bool

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Fetch new records from every configured connector and store them",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			log := newLogger(cfg.Log, stderr)

			// Connectors are built before the database is touched, so a
			// config mistake fails fast and changes nothing.
			conns, err := buildConnectors(cfg, log)
			if err != nil {
				return err
			}

			if !skipMigrations {
				version, err := store.Migrate(ctx, string(cfg.Database.URL), time.Duration(cfg.Database.Timeout))
				if err != nil {
					return err
				}
				log.Info("schema is up to date", "version", version)
			}
			db, err := store.Open(ctx, string(cfg.Database.URL), time.Duration(cfg.Database.Timeout))
			if err != nil {
				return err
			}
			defer db.Close()

			eng := engine.New(db, log)
			eng.Full = full
			report := eng.Sync(ctx, conns)

			writeReport(stdout, report)
			if report.Failed() {
				return errSyncFailed
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "path to the configuration file")
	cmd.Flags().BoolVar(&full, "full", false, "ignore stored cursors and re-read the whole history (existing rows are kept)")
	cmd.Flags().BoolVar(&skipMigrations, "skip-migrations", false, "do not apply pending schema migrations before syncing")
	return cmd
}

func migrateCommand(stdout io.Writer) *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply pending schema migrations and exit",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			version, err := store.Migrate(cmd.Context(), string(cfg.Database.URL), time.Duration(cfg.Database.Timeout))
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "schema is at version %d\n", version)
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "path to the configuration file")
	return cmd
}

func newLogger(cfg config.Log, w io.Writer) *slog.Logger {
	var level slog.Level
	// The level was validated when the config was loaded.
	_ = level.UnmarshalText([]byte(cfg.Level))
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
