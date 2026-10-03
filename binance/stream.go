// Package binance reads live trades from Binance's public market-data
// websocket. It needs no API key and cannot place orders.
package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// DefaultURL is Binance's public combined-stream endpoint.
const DefaultURL = "wss://stream.binance.com:9443/stream"

// Side is the side of the taker, the party whose order executed immediately.
type Side int

const (
	Buy  Side = iota // a buyer took liquidity, so the price was paid by a buyer
	Sell             // a seller took liquidity
)

func (s Side) String() string {
	if s == Buy {
		return "buy"
	}
	return "sell"
}

// Trade is one aggregated trade.
type Trade struct {
	Symbol string // e.g. "BTCUSDT"
	ID     int64
	Price  float64
	Qty    float64
	Time   time.Time // when the trade happened
	Side   Side
}

// aggTrade is the payload of an "<symbol>@aggTrade" stream.
//
// encoding/json matches keys case-insensitively when there is no exact
// match, and Binance sends both "e" and "E", and both "m" and "M". Every one
// of them is declared so each key lands in its own field; without EventTime
// and Ignore, "E" (a number) would overwrite Event and "M" (always true)
// would overwrite BuyerIsMaker, turning every trade into a sell.
type aggTrade struct {
	Event        string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	ID           int64  `json:"a"`
	Price        string `json:"p"`
	Qty          string `json:"q"`
	TradeTime    int64  `json:"T"` // milliseconds
	BuyerIsMaker bool   `json:"m"`
	Ignore       bool   `json:"M"`
}

type combined struct {
	Stream string   `json:"stream"`
	Data   aggTrade `json:"data"`
}

// ParseMessage decodes one combined-stream message.
func ParseMessage(b []byte) (Trade, error) {
	var msg combined
	if err := json.Unmarshal(b, &msg); err != nil {
		return Trade{}, fmt.Errorf("decoding message: %w", err)
	}
	d := msg.Data
	if d.Event != "aggTrade" {
		return Trade{}, fmt.Errorf("unexpected event %q", d.Event)
	}
	price, err := strconv.ParseFloat(d.Price, 64)
	if err != nil {
		return Trade{}, fmt.Errorf("price %q: %w", d.Price, err)
	}
	qty, err := strconv.ParseFloat(d.Qty, 64)
	if err != nil {
		return Trade{}, fmt.Errorf("quantity %q: %w", d.Qty, err)
	}
	// "m" is true when the buyer was the maker, i.e. a resting buy order was
	// hit by a seller: the taker sold. (The C# version this replaces showed
	// these trades as buys.)
	side := Buy
	if d.BuyerIsMaker {
		side = Sell
	}
	return Trade{
		Symbol: d.Symbol,
		ID:     d.ID,
		Price:  price,
		Qty:    qty,
		Time:   time.UnixMilli(d.TradeTime),
		Side:   side,
	}, nil
}

// StreamURL builds the combined-stream URL for the symbols' aggregated trades.
func StreamURL(base string, symbols []string) (string, error) {
	if len(symbols) == 0 {
		return "", errors.New("no symbols")
	}
	streams := make([]string, len(symbols))
	for i, s := range symbols {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" || strings.ContainsAny(s, "/@?&") {
			return "", fmt.Errorf("invalid symbol %q", symbols[i])
		}
		streams[i] = s + "@aggTrade"
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()
	// Binance expects the literal "/" separators and "@".
	u.RawQuery = strings.NewReplacer("%2F", "/", "%40", "@").Replace(u.RawQuery)
	return u.String(), nil
}

// Client streams trades, reconnecting with backoff when the connection drops.
// Binance closes every connection after 24 hours, so reconnecting is normal.
type Client struct {
	URL    string // combined-stream base URL; DefaultURL if empty
	Logger *slog.Logger

	// MinBackoff and MaxBackoff bound the wait between reconnects.
	MinBackoff, MaxBackoff time.Duration
}

// Stream sends the symbols' trades to out until ctx is cancelled, then closes
// out. Malformed messages are logged and skipped.
func (c *Client) Stream(ctx context.Context, symbols []string, out chan<- Trade) error {
	defer close(out)
	base := c.URL
	if base == "" {
		base = DefaultURL
	}
	u, err := StreamURL(base, symbols)
	if err != nil {
		return err
	}
	logger := c.Logger
	if logger == nil {
		logger = slog.Default()
	}
	minBackoff, maxBackoff := c.MinBackoff, c.MaxBackoff
	if minBackoff == 0 {
		minBackoff = 500 * time.Millisecond
	}
	if maxBackoff == 0 {
		maxBackoff = 30 * time.Second
	}

	backoff := minBackoff
	for {
		connected, err := c.session(ctx, u, out, logger)
		if ctx.Err() != nil {
			return nil
		}
		if connected {
			backoff = minBackoff // the connection worked; start over
		}
		wait := backoff/2 + rand.N(backoff/2+1) // jitter
		logger.Warn("stream disconnected, reconnecting", "err", err, "in", wait.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// session reads one connection until it fails. It reports whether it
// connected at all.
func (c *Client) session(ctx context.Context, u string, out chan<- Trade, logger *slog.Logger) (bool, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, u, nil)
	cancel()
	if err != nil {
		return false, err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	logger.Info("stream connected")

	for {
		_, msg, err := conn.Read(ctx) // also answers Binance's pings
		if err != nil {
			return true, err
		}
		trade, err := ParseMessage(msg)
		if err != nil {
			logger.Warn("skipping message", "err", err)
			continue
		}
		select {
		case out <- trade:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}
