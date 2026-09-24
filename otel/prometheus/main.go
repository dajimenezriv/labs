package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"time"
)

// What Alertmanager posts to a webhook, trimmed to the fields printed here.
type notification struct {
	Receiver    string            `json:"receiver"`
	Status      string            `json:"status"`
	GroupLabels map[string]string `json:"groupLabels"`
	Alerts      []struct {
		Status string            `json:"status"`
		Labels map[string]string `json:"labels"`
	} `json:"alerts"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		body, err := os.ReadFile("/lab/metrics.txt")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(body)
	})

	mux.HandleFunc("POST /{receiver}", func(w http.ResponseWriter, r *http.Request) {
		var n notification
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Printf("%s → %s %s %v (%d alerts)\n",
			time.Now().Format(time.TimeOnly), n.Receiver, n.Status, n.GroupLabels, len(n.Alerts))
		for _, a := range n.Alerts {
			// The group labels are already on the line above.
			labels := maps.Clone(a.Labels)
			maps.DeleteFunc(labels, func(k, _ string) bool { _, ok := n.GroupLabels[k]; return ok })
			fmt.Printf("           %-8s %v\n", a.Status, labels)
		}
	})

	if err := http.ListenAndServe(":8000", mux); err != nil {
		panic(err)
	}
}
