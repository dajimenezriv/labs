package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const metrics = `# TYPE kafka_consumer_lag gauge
kafka_consumer_lag{group="alerts",topic="sensor.readings",partition="0"} 0
kafka_consumer_lag{group="alerts",topic="sensor.readings",partition="1"} 0
kafka_consumer_lag{group="alerts",topic="sensor.readings",partition="2"} 5000
`

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
		io.WriteString(w, metrics)
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
