package proxy

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KibuuleNoah/ThirdRail/config"
)

type Route struct {
	PathPrefix  string
	Upstream    *url.URL
	StripPrefix bool
	Timeout     time.Duration // 0 → use gateway default
}

type Router struct {
	mu     sync.RWMutex
	routes []Route
}

func NewRouter(cfgRoutes []config.RouteConfig) (*Router, error) {
	routes, err := compileRoutes(cfgRoutes)
	if err != nil {
		return nil, err
	}
	return &Router{routes: routes}, nil
}

/* 
returns the best (longest-prefix) Route for the given request path,
*/
func (r *Router) Match(path string) (Route, error) {
	r.mu.RLock()
	routes := r.routes
	r.mu.RUnlock()

	for _, route := range routes {
		if strings.HasPrefix(path, route.PathPrefix) {
			return route, nil
		}
	}
	return Route{}, fmt.Errorf("no route matched path %q", path)
}

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

/*
returns a snapshot of the current routing table (read-only copy).
*/ 
func (r *Router) Routes() []Route {
	r.mu.RLock()
	cp := make([]Route, len(r.routes))
	copy(cp, r.routes)
	r.mu.RUnlock()
	return cp
}


/* 
parses config into runtime Routes, validates upstream URLs,
and sorts by descending prefix length for longest-match semantics.
*/
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
