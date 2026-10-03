package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// A real message from the live stream. "m" (buyer is maker) and "M" are both
// true; the trade is a sell.
const realMessage = `{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1791019853438,"s":"BTCUSDT","a":4080045245,"p":"84598.05000000","q":"0.00481000","f":6733597948,"l":6733597948,"T":1791019853438,"m":true,"M":true}}`

func message(symbol, price, qty string, buyerIsMaker bool, ms int64) string {
	m := "false"
	if buyerIsMaker {
		m = "true"
	}
	return `{"stream":"x","data":{"e":"aggTrade","E":` + itoa(ms) + `,"s":"` + symbol + `","a":1,"p":"` + price + `","q":"` + qty + `","T":` + itoa(ms) + `,"m":` + m + `,"M":true}}`
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestParseMessage(t *testing.T) {
	trade, err := ParseMessage([]byte(realMessage))
	require.NoError(t, err)
	require.Equal(t, "BTCUSDT", trade.Symbol)
	require.Equal(t, int64(4080045245), trade.ID)
	require.InDelta(t, 84598.05, trade.Price, 1e-9)
	require.InDelta(t, 0.00481, trade.Qty, 1e-12)
	require.Equal(t, time.UnixMilli(1791019853438), trade.Time)
	require.Equal(t, Sell, trade.Side, "buyer is maker: the taker sold")
}

// encoding/json matches keys case-insensitively: without explicit fields,
// "M" (always true) overwrote "m" and every trade became a sell.
func TestBuyerIsMakerIsNotOverwrittenByCapitalM(t *testing.T) {
	trade, err := ParseMessage([]byte(message("ETHUSDT", "2685.02", "0.5", false, 1)))
	require.NoError(t, err)
	require.Equal(t, Buy, trade.Side)
}

func TestParseMessageRejectsBadInput(t *testing.T) {
	for _, bad := range []string{
		`not json`,
		`{"data":{"e":"trade"}}`,
		strings.Replace(realMessage, `"p":"84598.05000000"`, `"p":"abc"`, 1),
		strings.Replace(realMessage, `"q":"0.00481000"`, `"q":""`, 1),
	} {
		_, err := ParseMessage([]byte(bad))
		require.Error(t, err, bad)
	}
}

func TestStreamURL(t *testing.T) {
	u, err := StreamURL(DefaultURL, []string{"BTCUSDT", " ethusdt "})
	require.NoError(t, err)
	require.Equal(t, "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade", u)

	for _, bad := range [][]string{nil, {""}, {"btc/usdt"}, {"btc@trade"}} {
		_, err := StreamURL(DefaultURL, bad)
		require.Error(t, err, "%q", bad)
	}
}

// fakeBinance serves a websocket that sends the given messages and then
// closes the connection, and counts connections.
func fakeBinance(t *testing.T, messages ...string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "btcusdt@aggTrade/ethusdt@aggTrade", r.URL.Query().Get("streams"))
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conns.Add(1)
		for _, m := range messages {
			if err := c.Write(r.Context(), websocket.MessageText, []byte(m)); err != nil {
				return
			}
		}
		c.Close(websocket.StatusNormalClosure, "bye") // like Binance's 24-hour cut-off
	}))
	t.Cleanup(srv.Close)
	return srv, &conns
}

func TestStreamReadsSkipsBadMessagesAndReconnects(t *testing.T) {
	srv, conns := fakeBinance(t,
		message("BTCUSDT", "100", "1", false, 1000),
		`{"garbage": true}`,
		message("ETHUSDT", "10", "2", true, 2000),
	)
	client := &Client{URL: "ws" + strings.TrimPrefix(srv.URL, "http"), MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan Trade)
	done := make(chan error, 1)
	go func() { done <- client.Stream(ctx, []string{"btcusdt", "ethusdt"}, out) }()

	var got []Trade
	for len(got) < 4 { // two trades per connection, so this needs a reconnect
		select {
		case tr := <-out:
			got = append(got, tr)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d trades, want 4", len(got))
		}
	}
	require.Equal(t, "BTCUSDT", got[0].Symbol)
	require.Equal(t, Buy, got[0].Side)
	require.Equal(t, "ETHUSDT", got[1].Symbol)
	require.Equal(t, Sell, got[1].Side)
	require.Equal(t, "BTCUSDT", got[2].Symbol, "after reconnecting the stream continues")
	require.GreaterOrEqual(t, conns.Load(), int32(2))

	cancel()
	require.NoError(t, <-done)
	for range out { // out is closed once Stream returns
	}
}

func TestStreamStopsWhileBinanceIsUnreachable(t *testing.T) {
	client := &Client{URL: "ws://127.0.0.1:1/stream", MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	out := make(chan Trade)
	require.NoError(t, client.Stream(ctx, []string{"btcusdt"}, out))
	_, open := <-out
	require.False(t, open)
}
