package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"golang.org/x/net/netutil"
)

// maxConnections is nginx's worker_connections, and it caps a different thing
// from the rate limiter: sockets rather than requests. A caller that opens
// five thousand connections and then sends nothing is never rate limited,
// because it never makes a request — but it has already taken five thousand
// file descriptors, and `ulimit -n` is 1024 on a lot of machines. That is the
// "too many open files" failure, and it takes the whole process down rather
// than the one caller responsible.
//
// 512 rather than nginx's 1024 because nginx counts both legs against that
// number: an in-flight request holds a client socket here and an upstream
// socket to identity. 512 in and 512 out is the same budget.
const maxConnections = 512

func main() {
	_ = godotenv.Load()

	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		panic("cannot load config: " + err.Error())
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{}))
	ctx := context.Background()

	handler, err := NewHandler(cfg, log)
	if err != nil {
		panic("new handler: " + err.Error())
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		// No ReadTimeout or WriteTimeout, unlike the services behind this one.
		// Both are deadlines on the whole request, and a gateway cannot know
		// how long a legitimate upload or a slow upstream should be allowed to
		// take. The bounds that matter here are on the upstream leg, and they
		// live on the transport.
		IdleTimeout: 60 * time.Second,
	}

	// The listener is built by hand rather than by ListenAndServe, which makes
	// its own and gives no way to wrap it.
	var lc net.ListenConfig

	ln, err := lc.Listen(context.Background(), "tcp", srv.Addr)
	if err != nil {
		panic("listen: " + err.Error())
	}

	// LimitListener does not refuse the excess — it stops accepting. The
	// connections past the cap wait in the kernel's accept queue, so a client
	// sees one that is slow to establish rather than one that was rejected,
	// and the kernel refuses them itself once that queue is full too. nginx
	// behaves the same way when a worker is out of connection slots.
	//
	// A keep-alive connection holds its slot while idle, which is what
	// IdleTimeout above is for: without it, 512 idle browsers would be the cap
	// reached and nothing would get in.
	ln = netutil.LimitListener(ln, maxConnections)

	go func() {
		log.InfoContext(ctx, "start gateway", "port", cfg.Port)

		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic("server serve: " + err.Error())
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.InfoContext(ctx, "shuting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.ErrorContext(shutdownCtx, "shutdown", "err", err)
	}
}
