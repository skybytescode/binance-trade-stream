// Package server serves the rolling statistics as JSON and as a live page.
package server

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/skybytescode/binance-trade-stream/window"
)

//go:embed index.html
var indexHTML []byte

// Snapshotter is satisfied by *window.Window.
type Snapshotter interface {
	Snapshot(now time.Time) []window.Stats
}

type response struct {
	Window string         `json:"window"`
	AsOf   time.Time      `json:"as_of"`
	Stats  []window.Stats `json:"stats"`
}

// Handler returns the HTTP routes. now is the clock, so tests can fix it.
func Handler(w Snapshotter, windowLength time.Duration, now func() time.Time) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.Write(indexHTML)
	})
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, r *http.Request) {
		rw.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /api/stats", func(rw http.ResponseWriter, r *http.Request) {
		t := now()
		writeJSON(rw, http.StatusOK, response{Window: windowLength.String(), AsOf: t, Stats: w.Snapshot(t)})
	})
	mux.HandleFunc("GET /api/stats/{symbol}", func(rw http.ResponseWriter, r *http.Request) {
		symbol := strings.ToUpper(r.PathValue("symbol"))
		for _, s := range w.Snapshot(now()) {
			if s.Symbol == symbol {
				writeJSON(rw, http.StatusOK, s)
				return
			}
		}
		writeJSON(rw, http.StatusNotFound, map[string]string{"error": "no trades for " + symbol + " in the window"})
	})
	return mux
}

func writeJSON(rw http.ResponseWriter, status int, v any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	enc := json.NewEncoder(rw)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
