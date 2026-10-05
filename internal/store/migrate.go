package store

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// The migrations ship inside the binary, so the schema a build expects is
// always the schema it can create.
//
//go:embed migrations/*.sql
var migrations embed.FS

// Migrate brings the schema at dsn up to the latest version and returns the
// version it ended on. It is safe to run on every start: applied migrations
// are skipped, and concurrent runs are serialised by an advisory lock.
func Migrate(dsn string) (uint, error) {
	// The connection is opened here rather than by handing the DSN to the
	// migration library, which keeps the password out of its error messages.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, fmt.Errorf("configure database: %w", err)
	}

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		db.Close()
		return 0, fmt.Errorf("connect to database: %w", err)
	}
	// Closing the driver releases its dedicated connection and closes db.
	defer driver.Close()
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return 0, fmt.Errorf("load migrations: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		return 0, fmt.Errorf("prepare migrations: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, fmt.Errorf("apply migrations: %w", err)
	}
	version, dirty, err := m.Version()
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	if dirty {
		return version, fmt.Errorf("schema is dirty at version %d: a migration failed midway and needs manual repair", version)
	}
	return version, nil
}
