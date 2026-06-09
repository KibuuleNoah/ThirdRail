package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KibuuleNoah/ThirdRail/config"
	"github.com/KibuuleNoah/ThirdRail/internal/middleware"
	"github.com/KibuuleNoah/ThirdRail/internal/proxy"
	"github.com/KibuuleNoah/ThirdRail/internal/resiliency"
)

func main() {
	//  Logger 
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	//  Config 
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	cfg.WithRoutes([]config.RouteConfig{
		{
			PathPrefix:  "/api/v1/users",
			UpstreamURL: envOr("USERS_SVC_URL", "http://localhost:9001"),
			StripPrefix: false,
			Timeout:     5 * time.Second,
		},
		{
			PathPrefix:  "/api/v1/orders",
			UpstreamURL: envOr("ORDERS_SVC_URL", "http://localhost:9002"),
			StripPrefix: false,
			Timeout:     10 * time.Second,
		},
		{
			PathPrefix:  "/api",
			UpstreamURL: envOr("DEFAULT_SVC_URL", "http://localhost:9000"),
			StripPrefix: false,
		},
	})

	//  Routing table 
	router, err := proxy.NewRouter(cfg.Routes)
	if err != nil {
		logger.Error("failed to build routing table", "error", err)
		os.Exit(1)
	}

	//  Reverse Proxy 
	proxyHandler := proxy.NewHandler(
		router,
		cfg.RequestTimeout,
		proxy.WithLogger(logger),
	)

	//  Resiliency — Circuit Breaker 
	cb := resiliency.NewCircuitBreaker(resiliency.CircuitBreakerConfig{
		Name:             "global",
		FailureThreshold: cfg.Resiliency.CBFailureThreshold,
		SuccessThreshold: cfg.Resiliency.CBSuccessThreshold,
		OpenTimeout:      cfg.Resiliency.CBOpenTimeout,
	})

	//  Resiliency — Retry 
	retryCfg := resiliency.RetryConfig{
		MaxRetries:  cfg.Resiliency.MaxRetries,
		BaseDelay:   cfg.Resiliency.RetryBaseDelay,
		MaxDelay:    cfg.Resiliency.RetryMaxDelay,
		Multiplier:  cfg.Resiliency.RetryMultiplier,
	}

	//  Build middleware chain 
	// Build from innermost to outermost.
	var handler http.Handler = proxyHandler

	// Retry wraps the proxy directly.
	handler = resiliency.RetryMiddleware(retryCfg, logger)(handler)

	// Circuit breaker wraps retry (so each retry attempt goes through the CB).
	handler = cb.Middleware(handler)

	// Timeout sets the outer deadline.
	handler = resiliency.TimeoutMiddleware(cfg.RequestTimeout, logger)(handler)

	// Rate limiter is the first line of defence.
	handler = middleware.RateLimiterMiddleware(middleware.RateLimiterConfig{
		RequestsPerSecond: cfg.RateLimit.RequestsPerSecond,
		BurstSize:         cfg.RateLimit.BurstSize,
		PerIP:             true,
	}, logger)(handler)

	// Logger is outermost so it captures the final response status.
	handler = middleware.LoggingMiddleware(logger, 8192)(handler)

	//  Admin / health endpoints 
	mux := http.NewServeMux()
	mux.Handle("/healthz", healthHandler(cb))
	mux.Handle("/", handler) // all other traffic goes through the gateway

	//  HTTP Server 
	srv := &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	//  Graceful shutdown 
	idleConnsClosed := make(chan struct{})
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		sig := <-quit

		logger.Info("shutdown signal received", "signal", sig.String())

		ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
		close(idleConnsClosed)
	}()

	logger.Info("ThirdRail gateway starting",
		"addr", cfg.Server.ListenAddr,
		"routes", len(cfg.Routes),
		"rps_limit", cfg.RateLimit.RequestsPerSecond,
		"burst", cfg.RateLimit.BurstSize,
	)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}

	<-idleConnsClosed
	logger.Info("ThirdRail gateway stopped cleanly")
}

// healthHandler returns a simple health-check endpoint that also exposes
// the current circuit breaker state for monitoring.
func healthHandler(cb *resiliency.CircuitBreaker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := cb.CurrentState()
		if state == resiliency.StateOpen {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"` + state.String() + `"}`))
	})
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
