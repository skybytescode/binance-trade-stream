package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skybytescode/binance-trade-stream/window"
)

type fixed []window.Stats

func (f fixed) Snapshot(time.Time) []window.Stats { return f }

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func TestAPI(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	h := Handler(fixed{{Symbol: "BTCUSDT", Trades: 3, Last: 84598.05}, {Symbol: "ETHUSDT", Trades: 1}}, time.Minute, func() time.Time { return now })

	rr := get(t, h, "/api/stats")
	require.Equal(t, http.StatusOK, rr.Code)
	var all struct {
		Window string         `json:"window"`
		AsOf   time.Time      `json:"as_of"`
		Stats  []window.Stats `json:"stats"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &all))
	require.Equal(t, "1m0s", all.Window)
	require.True(t, all.AsOf.Equal(now))
	require.Len(t, all.Stats, 2)

	rr = get(t, h, "/api/stats/btcusdt")
	require.Equal(t, http.StatusOK, rr.Code)
	var one window.Stats
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &one))
	require.Equal(t, 84598.05, one.Last)

	require.Equal(t, http.StatusNotFound, get(t, h, "/api/stats/dogeusdt").Code)
	require.Equal(t, http.StatusOK, get(t, h, "/healthz").Code)
	require.Contains(t, get(t, h, "/").Body.String(), "Live trades")
	require.Equal(t, http.StatusNotFound, get(t, h, "/nope").Code)
}
