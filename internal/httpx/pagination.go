package httpx

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Pages fetches first and then every page the server links to with
// rel="next", calling fn for each. Page numbers start at 1. It stops at the
// first error, from a request or from fn.
func (c *Client) Pages(ctx context.Context, first string, fn func(page int, r *Response) error) error {
	next := first
	for page := 1; next != ""; page++ {
		pageCtx := WithLogger(ctx, LoggerFrom(ctx, c.log).With("page", page))

		resp, err := c.Get(pageCtx, next)
		if err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if err := fn(page, resp); err != nil {
			return err
		}
		if next, err = nextURL(next, resp.Header); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
	}
	return nil
}

// nextURL resolves the rel="next" link against the page it came from. A link
// to another host is refused: following it would send our credentials there.
func nextURL(current string, h http.Header) (string, error) {
	link := NextLink(h)
	if link == "" {
		return "", nil
	}
	base, err := url.Parse(current)
	if err != nil {
		return "", fmt.Errorf("parse page url: %w", err)
	}
	ref, err := url.Parse(link)
	if err != nil {
		return "", fmt.Errorf("parse next link: %w", err)
	}
	next := base.ResolveReference(ref)
	if next.Scheme != base.Scheme || next.Host != base.Host {
		return "", fmt.Errorf("next link points to a different host (%s)", next.Host)
	}
	return next.String(), nil
}

// NextLink returns the target of the rel="next" entry of the Link header
// (RFC 8288), or "" when there is none, which means this is the last page.
func NextLink(h http.Header) string {
	for _, header := range h.Values("Link") {
		for _, entry := range splitLinks(header) {
			target, params, ok := strings.Cut(entry, ">")
			if !ok || !strings.HasPrefix(target, "<") {
				continue
			}
			if hasRel(params, "next") {
				return strings.TrimPrefix(target, "<")
			}
		}
	}
	return ""
}

// splitLinks splits a Link header on the commas that separate entries,
// ignoring commas inside a <target> or a "quoted" parameter.
func splitLinks(header string) []string {
	var entries []string
	var inTarget, inQuotes bool
	start := 0
	for i, r := range header {
		switch {
		case r == '"' && !inTarget:
			inQuotes = !inQuotes
		case r == '<' && !inQuotes:
			inTarget = true
		case r == '>' && !inQuotes:
			inTarget = false
		case r == ',' && !inTarget && !inQuotes:
			entries = append(entries, strings.TrimSpace(header[start:i]))
			start = i + 1
		}
	}
	return append(entries, strings.TrimSpace(header[start:]))
}

// hasRel reports whether the parameters of a link entry include the relation
// want. A rel value may list several relations separated by spaces.
func hasRel(params, want string) bool {
	for _, p := range strings.Split(params, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "rel") {
			continue
		}
		for _, rel := range strings.Fields(strings.Trim(strings.TrimSpace(value), `"`)) {
			if strings.EqualFold(rel, want) {
				return true
			}
		}
	}
	return false
}
