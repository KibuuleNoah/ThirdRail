// Package proxy provides the core HTTP routing and reverse-proxy logic.
package proxy

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KibuuleNoah/ThirdRail/gateway/config"
)

// Route is the resolved, runtime representation of a config.RouteConfig.
// It holds the pre-parsed upstream URL to avoid repeated parsing in the hot path.
type Route struct {
	PathPrefix  string
	Upstream    *url.URL
	StripPrefix bool
	Timeout     time.Duration // 0 → use gateway default
}

// Router is a thread-safe routing table that maps incoming request paths to
// upstream Route definitions using longest-prefix matching.
//
// Design notes:
//   - Routes are sorted by descending prefix length at load time so that
//     the first match in a linear scan is always the most-specific one.
//     This O(n) scan is fast enough for typical gateway routing tables
//     (tens of routes), and avoids the complexity of a trie with no measurable
//     benefit at that scale.
//   - A sync.RWMutex guards the route slice. Reads (Match) acquire RLock
//     so they never block each other; writes (Reload) acquire the exclusive
//     lock only during the pointer swap, which is nanoseconds.
type Router struct {
	mu     sync.RWMutex
	routes []Route
}

// NewRouter constructs a Router from the given config routes.
// Returns an error if any upstream URL cannot be parsed.
func NewRouter(cfgRoutes []config.RouteConfig) (*Router, error) {
	routes, err := compileRoutes(cfgRoutes)
	if err != nil {
		return nil, err
	}
	return &Router{routes: routes}, nil
}

// Match returns the best (longest-prefix) Route for the given request path,
// or an error if no route matches.
//
// This is called on every request so it must be allocation-free.
// strings.HasPrefix does not allocate; the RLock acquisition is a
// single atomic CAS on most platforms.
func (r *Router) Match(path string) (Route, error) {
	r.mu.RLock()
	routes := r.routes // local copy of slice header; elements are immutable
	r.mu.RUnlock()

	for _, route := range routes {
		if strings.HasPrefix(path, route.PathPrefix) {
			return route, nil
		}
	}
	return Route{}, fmt.Errorf("no route matched path %q", path)
}

// Reload atomically replaces the routing table. Safe to call at runtime
// (e.g. from a SIGHUP handler or admin API). In-flight requests continue
// using the old table until their Match() call returns; there is no torn read.
func (r *Router) Reload(cfgRoutes []config.RouteConfig) error {
	routes, err := compileRoutes(cfgRoutes)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.routes = routes
	r.mu.Unlock()
	return nil
}

// Routes returns a snapshot of the current routing table (read-only copy).
// Intended for admin/introspection endpoints.
func (r *Router) Routes() []Route {
	r.mu.RLock()
	cp := make([]Route, len(r.routes))
	copy(cp, r.routes)
	r.mu.RUnlock()
	return cp
}

//  internal 

// compileRoutes parses config into runtime Routes, validates upstream URLs,
// and sorts by descending prefix length for longest-match semantics.
func compileRoutes(cfgRoutes []config.RouteConfig) ([]Route, error) {
	routes := make([]Route, 0, len(cfgRoutes))

	for i, cr := range cfgRoutes {
		u, err := url.Parse(cr.UpstreamURL)
		if err != nil {
			return nil, fmt.Errorf("route[%d] %q: invalid upstream URL %q: %w",
				i, cr.PathPrefix, cr.UpstreamURL, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("route[%d] %q: upstream scheme must be http or https, got %q",
				i, cr.PathPrefix, u.Scheme)
		}
		// Normalise: remove any trailing slash from the upstream base so that
		// path concatenation produces clean URLs without double slashes.
		u.Path = strings.TrimRight(u.Path, "/")

		routes = append(routes, Route{
			PathPrefix:  cr.PathPrefix,
			Upstream:    u,
			StripPrefix: cr.StripPrefix,
			Timeout:     cr.Timeout,
		})
	}

	// Sort descending by prefix length → longest (most specific) prefix wins.
	sort.Slice(routes, func(i, j int) bool {
		return len(routes[i].PathPrefix) > len(routes[j].PathPrefix)
	})

	return routes, nil
}
