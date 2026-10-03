// Package window keeps rolling per-symbol trade statistics.
package window

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/skybytescode/binance-trade-stream/binance"
)

// Stats summarises one symbol's trades inside the window.
type Stats struct {
	Symbol      string    `json:"symbol"`
	Trades      int       `json:"trades"`
	Volume      float64   `json:"volume"`       // base asset, e.g. BTC
	QuoteVolume float64   `json:"quote_volume"` // quote asset, e.g. USDT
	BuyVolume   float64   `json:"buy_volume"`
	SellVolume  float64   `json:"sell_volume"`
	VWAP        float64   `json:"vwap"` // volume-weighted average price
	Last        float64   `json:"last"`
	High        float64   `json:"high"`
	Low         float64   `json:"low"`
	LastTrade   time.Time `json:"last_trade"`
}

// BuyRatio is the share of the volume bought by takers, from 0 to 1.
func (s Stats) BuyRatio() float64 {
	if s.Volume == 0 {
		return 0
	}
	return s.BuyVolume / s.Volume
}

// Window holds each symbol's trades from the last Duration, and at most
// MaxTrades of them per symbol when MaxTrades > 0. It is safe for concurrent
// use.
type Window struct {
	duration  time.Duration
	maxTrades int

	mu     sync.Mutex
	trades map[string][]binance.Trade // per symbol, oldest first
}

func New(duration time.Duration, maxTrades int) *Window {
	return &Window{duration: duration, maxTrades: maxTrades, trades: make(map[string][]binance.Trade)}
}

// Add records a trade. Trades are expected roughly in time order, as the
// exchange sends them; an older trade is still placed correctly.
func (w *Window) Add(t binance.Trade) {
	w.mu.Lock()
	defer w.mu.Unlock()

	ts := w.trades[t.Symbol]
	i := len(ts)
	for i > 0 && ts[i-1].Time.After(t.Time) {
		i--
	}
	ts = append(ts, binance.Trade{})
	copy(ts[i+1:], ts[i:])
	ts[i] = t
	if w.maxTrades > 0 && len(ts) > w.maxTrades {
		ts = ts[len(ts)-w.maxTrades:] // keep the newest
	}
	w.trades[t.Symbol] = ts
}

// Snapshot drops trades older than the window relative to now and returns
// every symbol's statistics, sorted by symbol.
func (w *Window) Snapshot(now time.Time) []Stats {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := now.Add(-w.duration)
	out := make([]Stats, 0, len(w.trades))
	for symbol, ts := range w.trades {
		keep := sort.Search(len(ts), func(i int) bool { return !ts[i].Time.Before(cutoff) })
		if keep > 0 {
			// Copy so the dropped trades can be garbage collected.
			ts = append([]binance.Trade(nil), ts[keep:]...)
			w.trades[symbol] = ts
		}
		out = append(out, summarise(symbol, ts))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

func summarise(symbol string, ts []binance.Trade) Stats {
	s := Stats{Symbol: symbol, Trades: len(ts)}
	if len(ts) == 0 {
		return s
	}
	s.Low = math.Inf(1)
	for _, t := range ts {
		s.Volume += t.Qty
		s.QuoteVolume += t.Price * t.Qty
		if t.Side == binance.Buy {
			s.BuyVolume += t.Qty
		} else {
			s.SellVolume += t.Qty
		}
		s.High = max(s.High, t.Price)
		s.Low = min(s.Low, t.Price)
	}
	last := ts[len(ts)-1]
	s.Last, s.LastTrade = last.Price, last.Time
	if s.Volume > 0 {
		s.VWAP = s.QuoteVolume / s.Volume
	}
	// Binance quotes at most 8 decimals; drop the floating-point noise that
	// summing adds (1.8965499999999997 -> 1.89655).
	for _, v := range []*float64{&s.Volume, &s.QuoteVolume, &s.BuyVolume, &s.SellVolume, &s.VWAP} {
		*v = round8(*v)
	}
	return s
}

func round8(x float64) float64 { return math.Round(x*1e8) / 1e8 }
