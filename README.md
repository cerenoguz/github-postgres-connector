# GitHub → Postgres connector

A command-line tool that reads every commit on the default branch of the
configured GitHub repositories and stores them in PostgreSQL. The first run
loads the full history; later runs load only what is new.

It is written as the first of several connectors: everything that is not
specific to GitHub (retries, rate limits, pagination, auth, cursors,
persistence, reporting) lives in shared packages meant to be reused by a
GitLab or Jira connector. [Adding a second connector](#adding-a-second-connector)
says exactly what would and would not change.

```
connector sync --config config.yaml
```

## Run it

Requirements: Go 1.26+, and Docker for the local database and the integration
tests.

```bash
make up                                   # PostgreSQL on localhost:5432
cp config.example.yaml config.yaml        # then edit the repository list
export DATABASE_URL='postgres://connector:connector@localhost:5432/connector?sslmode=disable'
export GITHUB_TOKEN=...                   # a personal access token
make sync
```

The tool reads those two variables from the environment and does not load
`.env` files itself; [.env.example](.env.example) only lists them.

The token only needs to read commits. A fine-grained personal access token
with read-only "Contents" access to the listed repositories is enough; public
repositories need no permissions at all.

`sync` applies any pending schema migrations, syncs each repository and prints
a report:

```
CONNECTOR  RESOURCE                          STATUS  READ  INSERTED  DURATION
github     golang/example                    ok      76    76        925ms
github     octocat/Hello-World               ok      3     3         632ms
github     octocat/this-repo-does-not-exist  failed  0     0         231ms

3 resources, 1 failed, 79 records read, 79 inserted

Errors:
  github octocat/this-repo-does-not-exist: resolve head: GET https://api.github.com/...: HTTP 404: ...
```

A resource is whatever a connector syncs independently; for GitHub it is a
repository. The report goes to stdout and structured logs go to stderr.

| Exit code | Meaning |
| --- | --- |
| 0 | Every repository synced |
| 1 | The run happened, but at least one repository failed or was cancelled |
| 2 | The run could not start (bad flags, bad config, database unreachable) |

Other commands and flags:

| Command | Purpose |
| --- | --- |
| `connector sync --full` | Ignore stored cursors and re-read the whole history. Existing rows are kept, so it is safe to use as a repair. |
| `connector sync --skip-migrations` | Do not touch the schema, for setups where migrations are a separate deploy step. |
| `connector migrate` | Apply pending migrations and exit. |

To try it without a token, set `auth: {type: none}` in the config. GitHub then
allows 60 requests per hour, which is enough for a few small public
repositories.

### Configuration

See [config.example.yaml](config.example.yaml) for every key. Two rules:

- Secrets are referenced as `${NAME}` and read from the environment. A
  reference to an unset variable is an error, never an empty string.
- Unknown keys are an error at every level, including inside a connector
  entry, so a typo such as `fetch_stat` does not silently fall back to a
  default.

### Tests

```bash
make test        # everything; starts PostgreSQL in Docker via testcontainers
make test-unit   # only the tests that need nothing but Go
```

The integration tests skip themselves, with a message, when Docker is not
available. No test calls the real GitHub.

`make lint` runs `gofmt`, `go vet` and `staticcheck`. A GitHub Actions
workflow checks formatting, vets and runs every test, including the
PostgreSQL ones, on each push.

## Design

```
cmd/connector            main: signal handling, exit code
internal/
  app                    CLI commands, wiring, report, connector factories
  config                 YAML loading and validation
  connector              the contract: Connector, Commit, Batch, Cursor
  engine                 runs connectors; owns cursor and failure rules
  httpx                  shared HTTP client: retries, rate limits, pagination
  auth                   Authenticator implementations and the Secret type
  store                  PostgreSQL: migrations, idempotent insert, cursors
  connectors/github      the only package that knows GitHub
    githubtest           a fake GitHub API server for tests
  store/storetest        starts PostgreSQL in Docker for tests
```

Dependencies point inwards. `connectors/github` imports the contract and the
HTTP client; it does not import the store, the engine or the config. The
engine imports only the contract.

### The connector contract

```go
type Connector interface {
    Name() string                 // "github"
    Resources() []string          // "owner/repo", synced independently
    Fetch(ctx, resource, since Cursor, emit EmitFunc) (next Cursor, err error)
}
```

A connector emits pages of normalized records and returns the cursor to resume
from. Three properties make it reusable:

- **The cursor is opaque.** The engine stores it and hands it back, nothing
  more. GitHub uses a commit SHA; another tool can use a timestamp or a page
  token.
- **The engine persists, not the connector.** `emit` hands a page to the
  engine, which is why consistency rules are written once.
- **Records are normalized.** GitLab commits would map to the same `Commit`
  type and land in the same table, distinguished by the `source` column.

### Adding a second connector

1. Create `internal/connectors/gitlab` implementing `Connector`.
2. If its rate-limit signals differ from plain HTTP, write a
   `httpx.Classifier` for it (GitHub's is about 30 lines).
3. Register a factory in `internal/app/connectors.go` (one map entry plus a
   function that decodes its config keys).

Nothing in `engine`, `httpx`, `auth`, `store` or `connectors/github` changes.

That claim has limits, and with only one connector built it is a design
intention rather than something proven:

- The engine and the report count records through `Batch.Len()` and speak of
  "resources", so they hold no commit-specific logic. The store does.
- `httpx` only does GET and only paginates through the `Link` header. That
  fits GitHub, GitLab and Jira's REST API. Linear is GraphQL: it needs POST
  and cursor pagination in the response body, so its connector would add a
  request method and its own paging loop, while still reusing auth, retries,
  rate-limit waits and timeouts.

A connector for a tool with a **new kind of record** (Jira issues) also needs
a field on `connector.Batch` (counted by its `Len` and `CheckResource`
methods), a table and a store method. Existing connectors
still do not change. I chose typed fields over a generic `Record` interface:
it costs a shared-code change per entity kind, and in exchange the compiler
checks every mapping and every insert.

### Incremental sync: head SHA, not commit date

The brief suggests the date of the last loaded commit as the cursor. I started
there and moved away from it, because it loses data in an everyday situation.

A commit's date is when it was written, not when it reached the branch. A pull
request merged today, with a merge commit, brings in commits dated last week.
If a sync ran in between, a `since=<last date>` filter never returns them, and
no overlap window fixes that in general.

Instead, the cursor is **the SHA of the default branch's head at the last
successful sync**. A run:

1. Resolves the current head (one request).
2. If it equals the cursor, stops. A run with nothing new costs one request.
3. Otherwise asks GitHub for the commits reachable from the new head but not
   from the old one (`GET /compare/{old}...{new}`, i.e. `git log old..new`).
4. With no cursor, walks the whole history from that head.

Pinning the run to one head SHA also keeps pagination stable while people keep
pushing, and makes the new cursor exact.

The connector falls back to a full walk in two cases, both safe because
existing rows are skipped:

- The old head no longer exists (a force-push followed by garbage
  collection), which GitHub reports as a 404.
- The range is too large for one comparison. GitHub lists at most 10,000
  commits and drops the oldest beyond that, so the connector checks the first
  page for a mismatch before storing anything. See [Trade-offs](#trade-offs).

### Consistency

- Each page is written in **one transaction** with
  `ON CONFLICT (source, repository, sha) DO NOTHING`. The number of rows
  actually inserted is what the report shows as INSERTED.
- The **cursor is saved only after `Fetch` returns without error**, which is
  after the last page is stored.
- So after a crash, a failure or Ctrl+C: stored pages stay, the cursor does
  not move, and the next run re-reads from the old cursor and skips what is
  already there. Nothing is lost and nothing is duplicated. An end-to-end test
  fails a sync on its second page and checks the next run finishes it.

The engine also refuses a page containing a record that belongs to a different
resource than the one being synced. Rows are keyed by repository, so without
that check a bug in one connector could write into another repository's data.

The key includes `source` in addition to the repository and SHA the brief asks
for, so that `acme/widgets` on GitHub and on GitLab cannot collide.

## Non-functional requirements

| Topic | Approach |
| --- | --- |
| Authorization | The GitHub base URL must be `https` (plain `http` is accepted only for a server on the local machine), so the token cannot be sent in clear text. `auth.Authenticator` has one method, `Apply(*http.Request)`, called by the HTTP client on every attempt. Bearer (PAT), Basic and None exist; a GitHub App is another implementation, and calling it per attempt leaves room for token refresh. Extraction code never sees a credential. The token is held in `auth.Secret`, which prints `[REDACTED]` under `fmt`, `slog` and JSON, and it is only ever placed in a header, never a URL. Tests assert it is absent from logs, errors and command output. |
| Retries | In `httpx.Client.Get`. Network errors, per-attempt timeouts, 429 and 500/502/503/504 are retried up to `max_retries` with exponential backoff; the upper half of each delay is random. Every other status is returned at once as a `StatusError`. |
| Rate limiting | `Retry-After` is handled generically. GitHub's rules are a `Classifier` in the GitHub package: 403/429 with `X-RateLimit-Remaining: 0` waits until `X-RateLimit-Reset`; a 403 without those signals is a permission error and is not retried. When a *successful* response reports zero remaining, the next request is delayed until the reset, so the limit is not hit at all. Waits longer than `max_wait` (65 minutes by default, one GitHub quota window plus a margin) fail instead of hanging. |
| Pagination | `httpx.Client.Pages` follows `rel="next"` from the `Link` header until there is none. Only the first URL is built in code. A next link pointing at a different host is refused, since following it would send the token there. |
| Timeouts and cancellation | Each HTTP attempt has its own timeout, covering the body read, and a response body is capped at 64 MiB. Each database operation has one too (`database.timeout`, 30s by default), so a database that stops answering fails the repository instead of hanging the run. Ctrl+C cancels the context: the request in flight and any backoff sleep stop immediately, the open transaction rolls back, remaining repositories are reported as cancelled, and the exit code is 1. A second Ctrl+C kills the process. |
| Consistency | See above. |
| Fault isolation | The engine records a repository's error and moves on. Config mistakes are different: they stop the run before the database is touched. |
| Logging | `log/slog`, JSON by default. Lines carry `connector` and `resource`; page lines add `page`; retry lines add `attempt`, `wait` and whether it was a rate limit. |
| Tests | Retry, backoff, timeout, cancellation and `Link` parsing against `httptest` servers. The GitHub connector against a fake GitHub whose pagination links are opaque, so hand-built page URLs would fail. Mapping against a JSON fixture. The store, and the whole command, against PostgreSQL in Docker. |

## Trade-offs

- **GitHub lists at most 10,000 commits per comparison** and silently drops
  the oldest beyond that; I measured this on a 12,000-commit range of
  `golang/go`. The first page reports both the true size of the range and how
  many commits will be listed, so the connector detects the mismatch before
  storing anything and reads the full history instead. A repository that
  gains more than 10,000 commits between two runs therefore costs a full
  walk rather than losing data.
- **A very large first sync is fragile.** Because the cursor moves only at the
  end, a repository that needs more requests than the hourly quota must get
  through one or more rate-limit waits without any permanent error, or it
  starts again from the top.
- **Compare responses are heavier.** GitHub includes file diffs on the first
  page of a comparison. Incremental runs are usually small, so I accepted it
  in exchange for correctness.
- **An interrupted first sync starts over**, for the same reason. It repeats
  its API calls next time; no data is lost or duplicated.
- **Rows are never updated.** Commits are immutable, so `DO NOTHING` is right
  for everything except line stats: enabling `fetch_stats` later does not
  backfill rows stored without them.
- **Line stats cost one request per commit** and are off by default. Without
  stats a 10,000-commit repository needs about 100 requests; with them, 10,100.
- **A rate-limit wait uses one of the retries.** One bounded loop is simpler
  than two counters. Several rate-limit hits on the same request could exhaust
  it; the repository then fails and the next run resumes.
- **A secondary rate limit without `Retry-After`** looks like a plain 403 and
  is treated as permanent for that run, as the brief specifies for 403s.
- **Repositories sync one after another.** They share one rate limit, so
  parallelism buys less than it seems and makes waits harder to reason about.
- **Responses are read into memory.** Pages are at most 100 commits, and it
  lets the timeout cover the body.
- **Only the default branch**, as the brief asks. Commits that exist solely on
  other branches are not loaded.
- **History rewritten by a force-push leaves the old rows in place.** The
  table records commits that were once on the branch.
- **Repository names are taken as written.** GitHub ignores case and follows
  renames, but rows and cursors are keyed by the name in the config, so
  `Acme/Widgets` and `acme/widgets`, or a repository before and after a
  rename, are stored as two. Keying on GitHub's numeric repository ID would
  fix it.
- **Two runs at the same time are safe but not coordinated.** Idempotent
  inserts prevent duplicates, yet both runs do the same work and split the
  INSERTED counts between them. There is no lock.
- **Only the mapped fields are stored**, not GitHub's raw payload. Changing
  the mapping later means fetching again.

## With more time

- Checkpoint a long first sync so it can resume midway.
- Sync repositories concurrently with a shared rate-limit budget.
- Backfill line stats for rows stored without them; consider GraphQL, which
  returns stats in the list query and removes the per-commit request.
- GitHub App authentication (installation tokens that refresh), which the
  per-attempt `Authenticator` call was shaped for.
- A `sync_runs` table recording each run's counts and errors, and metrics.
- Discover repositories from an organisation instead of listing them, and key
  them by GitHub's repository ID.
- Keep the raw API payload next to the mapped columns.
- A second connector, to test the contract against a real difference rather
  than an imagined one.
- A container image, and scheduling.
