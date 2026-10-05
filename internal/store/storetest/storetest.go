// Package storetest starts a real PostgreSQL for integration tests.
package storetest

import (
	"context"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	once         sync.Once
	containerDSN string
	containerErr error
)

// DSN returns the connection string of a PostgreSQL container shared by the
// tests of one package. The test is skipped with -short and when Docker is
// not available. The database starts empty and unmigrated.
func DSN(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	once.Do(func() {
		ctx := context.Background()
		ctr, err := postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase("connector"),
			postgres.WithUsername("connector"),
			postgres.WithPassword("connector"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			containerErr = err
			return
		}
		// The container is removed by testcontainers' reaper when the test
		// process exits.
		containerDSN, containerErr = ctr.ConnectionString(ctx, "sslmode=disable")
	})
	if containerErr != nil {
		t.Fatalf("start postgres: %v", containerErr)
	}
	return containerDSN
}
