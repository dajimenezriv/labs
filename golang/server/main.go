package main

import (
	"errors"
	"net/http"
	"time"
)

func main() {
	s := server{}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /sites", s.getSites)

	srv := &http.Server{
		Addr:              ":8000",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic("listen and serve: " + err.Error())
	}
}

type server struct{}

func (s *server) getSites(w http.ResponseWriter, r *http.Request) {

}
