package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	cfg := Config{
		CookieSecure:  false,
		Port:          8000,
		JWTPrivateKey: "privateKey",
	}

	signingKey, err := cfg.SigningKey()
	if err != nil {
		panic("signing key: " + err.Error())
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{}))

	s := NewServer(cfg, log, NewQueries(), NewIssuer(signingKey, 15*time.Minute))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: s.Handler(),
	}

	go func() {
		log.Info("start server", "port", cfg.Port)

		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic("server listen: " + err.Error())
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Info("shuting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "shutdown", "err", err)
	}
}
