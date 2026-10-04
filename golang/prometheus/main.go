// docker compose up -d --force-recreate -V app prometheus

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// What Alertmanager posts to a webhook, trimmed to the fields printed here.
type notification struct {
	Alerts []struct {
		Status string            `json:"status"`
		Labels map[string]string `json:"labels"`
	} `json:"alerts"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		io.WriteString(w, counter())
	})

	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		var n notification
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, a := range n.Alerts {
			fmt.Println(time.Now().Format(time.TimeOnly), a.Status, a.Labels)
		}
	})

	fmt.Println("listening on :8000, waiting for notifications")
	if err := http.ListenAndServe(":8000", mux); err != nil {
		panic(err)
	}
}

func counter() string {
	var metrics strings.Builder
	metrics.WriteString("# TYPE my_counter counter\n")
	values := []int{1, 2, 3, 4, 5, 5, 5, 5, 5, 9, 10, 11}

	ts := time.Now().Add(-time.Hour)
	for _, v := range values {
		for range 4 {
			fmt.Fprintf(&metrics, "my_counter %d %d\n", v*18000, ts.UnixMilli())
			ts = ts.Add(15 * time.Second)
		}
	}

	return metrics.String()
}
