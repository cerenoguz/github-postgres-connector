package httpx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cerenoguz/github-postgres-connector/internal/auth"
)

func TestNextLink(t *testing.T) {
	tests := []struct {
		name string
		link []string
		want string
	}{
		{"no header", nil, ""},
		{
			"github style",
			[]string{`<https://api.test/r?page=2>; rel="next", <https://api.test/r?page=9>; rel="last"`},
			"https://api.test/r?page=2",
		},
		{
			"last page has no next",
			[]string{`<https://api.test/r?page=1>; rel="first", <https://api.test/r?page=8>; rel="prev"`},
			"",
		},
		{
			"next is not the first entry",
			[]string{`<https://api.test/r?page=1>; rel="prev", <https://api.test/r?page=3>; rel="next"`},
			"https://api.test/r?page=3",
		},
		{
			"comma inside the target",
			[]string{`<https://api.test/r?ids=1,2&page=2>; rel="next"`},
			"https://api.test/r?ids=1,2&page=2",
		},
		{
			"comma inside a quoted parameter",
			[]string{`<https://api.test/a>; rel="prev"; title="one, two", <https://api.test/b>; rel="next"`},
			"https://api.test/b",
		},
		{"unquoted rel", []string{`<https://api.test/r?page=2>; rel=next`}, "https://api.test/r?page=2"},
		{"several relations", []string{`<https://api.test/r?page=2>; rel="last next"`}, "https://api.test/r?page=2"},
		{
			"split across header lines",
			[]string{`<https://api.test/r?page=1>; rel="prev"`, `<https://api.test/r?page=3>; rel="next"`},
			"https://api.test/r?page=3",
		},
		{"malformed", []string{`https://api.test/r?page=2; rel="next"`}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tt.link {
				h.Add("Link", v)
			}
			if got := NextLink(h); got != tt.want {
				t.Errorf("NextLink = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPagesFollowsNextLinksToTheLastPage(t *testing.T) {
	var requested []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.RequestURI())
		// The server chooses opaque cursors; the client must not guess them.
		switch r.URL.Query().Get("after") {
		case "":
			w.Header().Set("Link", fmt.Sprintf(`<%s/items?after=abc>; rel="next"`, srv.URL))
			w.Write([]byte("1"))
		case "abc":
			w.Header().Set("Link", `</items?after=xyz>; rel="next"`) // relative link
			w.Write([]byte("2"))
		default:
			w.Write([]byte("3"))
		}
	}))
	t.Cleanup(srv.Close)
	c, _ := testClient(Options{})

	var pages []int
	var bodies []string
	err := c.Pages(context.Background(), srv.URL+"/items", func(page int, r *Response) error {
		pages = append(pages, page)
		bodies = append(bodies, string(r.Body))
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(bodies, ","); got != "1,2,3" {
		t.Errorf("bodies = %s, want 1,2,3", got)
	}
	if len(pages) != 3 || pages[0] != 1 || pages[2] != 3 {
		t.Errorf("pages = %v, want 1 2 3", pages)
	}
	want := "/items,/items?after=abc,/items?after=xyz"
	if got := strings.Join(requested, ","); got != want {
		t.Errorf("requested = %s, want %s", got, want)
	}
}

func TestPagesStopsWhenTheCallbackFails(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Link", `</next>; rel="next"`)
	}))
	t.Cleanup(srv.Close)
	c, _ := testClient(Options{})
	boom := errors.New("boom")

	err := c.Pages(context.Background(), srv.URL, func(int, *Response) error { return boom })

	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want no request after the failure", calls)
	}
}

func TestPagesReportsWhichPageFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/two" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Link", `</two>; rel="next"`)
	}))
	t.Cleanup(srv.Close)
	c, _ := testClient(Options{})

	err := c.Pages(context.Background(), srv.URL, func(int, *Response) error { return nil })

	var se *StatusError
	if !errors.As(err, &se) || se.Status != 404 || !strings.Contains(err.Error(), "page 2") {
		t.Errorf("err = %v, want a 404 attributed to page 2", err)
	}
}

func TestPagesRefusesNextLinkToAnotherHost(t *testing.T) {
	const token = "ghp_supersecret"
	leaked := false
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		leaked = leaked || r.Header.Get("Authorization") != ""
	}))
	t.Cleanup(other.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<%s/steal>; rel="next"`, other.URL))
	}))
	t.Cleanup(srv.Close)
	bearer, _ := auth.Bearer(auth.NewSecret(token))
	c, _ := testClient(Options{Auth: bearer})

	err := c.Pages(context.Background(), srv.URL, func(int, *Response) error { return nil })

	if err == nil || !strings.Contains(err.Error(), "different host") {
		t.Errorf("err = %v, want the link to be refused", err)
	}
	if leaked {
		t.Error("credentials were sent to another host")
	}
}
