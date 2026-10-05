package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/cerenoguz/github-postgres-connector/internal/connector"
)

// memStore is an in-memory Store keyed the same way the real one is.
type memStore struct {
	cursors map[string]connector.Cursor
	commits map[string]bool
	saveErr error
}

func newMemStore() *memStore {
	return &memStore{cursors: map[string]connector.Cursor{}, commits: map[string]bool{}}
}

func (s *memStore) Cursor(_ context.Context, source, resource string) (connector.Cursor, error) {
	return s.cursors[source+"/"+resource], nil
}

func (s *memStore) SaveBatch(_ context.Context, source string, b connector.Batch) (int, error) {
	if s.saveErr != nil {
		return 0, s.saveErr
	}
	inserted := 0
	for _, c := range b.Commits {
		key := source + "/" + c.Repository + "@" + c.SHA
		if !s.commits[key] {
			s.commits[key] = true
			inserted++
		}
	}
	return inserted, nil
}

func (s *memStore) SaveCursor(_ context.Context, source, resource string, c connector.Cursor) error {
	s.cursors[source+"/"+resource] = c
	return nil
}

// fakeConnector serves canned pages per resource and can fail after them.
type fakeConnector struct {
	pages     map[string][][]string // resource -> pages of SHAs
	failAfter map[string]error      // resource -> error returned after its pages
	next      connector.Cursor
	gotSince  map[string]connector.Cursor
	order     []string
	onFetch   func(resource string)
}

func (f *fakeConnector) Name() string        { return "fake" }
func (f *fakeConnector) Resources() []string { return f.order }

func (f *fakeConnector) Fetch(ctx context.Context, resource string, since connector.Cursor, emit connector.EmitFunc) (connector.Cursor, error) {
	if f.gotSince == nil {
		f.gotSince = map[string]connector.Cursor{}
	}
	f.gotSince[resource] = since
	if f.onFetch != nil {
		f.onFetch(resource)
	}
	for i, shas := range f.pages[resource] {
		b := connector.Batch{Page: i + 1}
		for _, sha := range shas {
			b.Commits = append(b.Commits, connector.Commit{SHA: sha, Repository: resource})
		}
		if err := emit(ctx, b); err != nil {
			return "", err
		}
	}
	if err := f.failAfter[resource]; err != nil {
		return "", err
	}
	return f.next, nil
}

func newEngine(s Store) *Engine {
	return New(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestSyncStoresPagesAndAdvancesCursor(t *testing.T) {
	store := newMemStore()
	conn := &fakeConnector{
		order: []string{"a/b"},
		pages: map[string][][]string{"a/b": {{"1", "2"}, {"3"}}},
		next:  "c1",
	}

	report := newEngine(store).Sync(context.Background(), []connector.Connector{conn})

	if report.Failed() {
		t.Fatalf("unexpected failure: %v", report.Results[0].Err)
	}
	if got := report.Results[0]; got.Read != 3 || got.Inserted != 3 {
		t.Errorf("read/inserted = %d/%d, want 3/3", got.Read, got.Inserted)
	}
	if got := store.cursors["fake/a/b"]; got != "c1" {
		t.Errorf("cursor = %q, want c1", got)
	}
}

func TestSyncTwiceInsertsNothingAndResumesFromCursor(t *testing.T) {
	store := newMemStore()
	conn := &fakeConnector{
		order: []string{"a/b"},
		pages: map[string][][]string{"a/b": {{"1", "2"}}},
		next:  "c1",
	}
	eng := newEngine(store)
	eng.Sync(context.Background(), []connector.Connector{conn})

	report := eng.Sync(context.Background(), []connector.Connector{conn})

	if got := report.Results[0]; got.Read != 2 || got.Inserted != 0 {
		t.Errorf("read/inserted = %d/%d, want 2/0", got.Read, got.Inserted)
	}
	if got := conn.gotSince["a/b"]; got != "c1" {
		t.Errorf("second run started from %q, want c1", got)
	}
}

func TestFailureKeepsCursorAndDoesNotStopOtherResources(t *testing.T) {
	store := newMemStore()
	store.cursors["fake/bad/repo"] = "old"
	boom := errors.New("boom")
	conn := &fakeConnector{
		order:     []string{"bad/repo", "good/repo"},
		pages:     map[string][][]string{"bad/repo": {{"1"}}, "good/repo": {{"2"}}},
		failAfter: map[string]error{"bad/repo": boom},
		next:      "new",
	}

	report := newEngine(store).Sync(context.Background(), []connector.Connector{conn})

	if !report.Failed() {
		t.Fatal("report should be failed")
	}
	bad, good := report.Results[0], report.Results[1]
	if !errors.Is(bad.Err, boom) {
		t.Errorf("bad.Err = %v, want boom", bad.Err)
	}
	if bad.Read != 1 || bad.Inserted != 1 {
		t.Errorf("bad read/inserted = %d/%d, want the stored page to be counted", bad.Read, bad.Inserted)
	}
	if got := store.cursors["fake/bad/repo"]; got != "old" {
		t.Errorf("failed resource cursor = %q, want it unchanged", got)
	}
	if good.Err != nil || good.Inserted != 1 {
		t.Errorf("good = %+v, want it synced", good)
	}
	if got := store.cursors["fake/good/repo"]; got != "new" {
		t.Errorf("good cursor = %q, want new", got)
	}
}

func TestStoreErrorFailsResourceWithoutAdvancingCursor(t *testing.T) {
	store := newMemStore()
	store.saveErr = errors.New("db down")
	conn := &fakeConnector{
		order: []string{"a/b"},
		pages: map[string][][]string{"a/b": {{"1"}}},
		next:  "c1",
	}

	report := newEngine(store).Sync(context.Background(), []connector.Connector{conn})

	if !errors.Is(report.Results[0].Err, store.saveErr) {
		t.Errorf("err = %v, want db down", report.Results[0].Err)
	}
	if _, ok := store.cursors["fake/a/b"]; ok {
		t.Error("cursor advanced despite the store error")
	}
}

func TestCancellationSkipsRemainingResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &fakeConnector{
		order:   []string{"first/repo", "second/repo"},
		pages:   map[string][][]string{"first/repo": {{"1"}}, "second/repo": {{"2"}}},
		onFetch: func(string) { cancel() },
	}

	report := newEngine(newMemStore()).Sync(ctx, []connector.Connector{conn})

	if len(report.Results) != 2 {
		t.Fatalf("got %d results, want every resource reported", len(report.Results))
	}
	if !errors.Is(report.Results[1].Err, context.Canceled) {
		t.Errorf("second.Err = %v, want context.Canceled", report.Results[1].Err)
	}
	if _, fetched := conn.gotSince["second/repo"]; fetched {
		t.Error("second resource was fetched after cancellation")
	}
}

func TestFullIgnoresStoredCursor(t *testing.T) {
	store := newMemStore()
	store.cursors["fake/a/b"] = "old"
	conn := &fakeConnector{order: []string{"a/b"}, next: "new"}
	eng := newEngine(store)
	eng.Full = true

	eng.Sync(context.Background(), []connector.Connector{conn})

	if got := conn.gotSince["a/b"]; got != "" {
		t.Errorf("since = %q, want empty on a full sync", got)
	}
	if got := store.cursors["fake/a/b"]; got != "new" {
		t.Errorf("cursor = %q, want new", got)
	}
}
