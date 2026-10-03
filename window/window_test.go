package window

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skybytescode/binance-trade-stream/binance"
)

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func trade(symbol string, price, qty float64, side binance.Side, at time.Duration) binance.Trade {
	return binance.Trade{Symbol: symbol, Price: price, Qty: qty, Side: side, Time: t0.Add(at)}
}

func TestStats(t *testing.T) {
	w := New(time.Minute, 0)
	w.Add(trade("BTCUSDT", 100, 1, binance.Buy, 0))
	w.Add(trade("BTCUSDT", 110, 3, binance.Sell, time.Second))
	w.Add(trade("BTCUSDT", 90, 1, binance.Buy, 2*time.Second))

	stats := w.Snapshot(t0.Add(3 * time.Second))
	require.Len(t, stats, 1)
	s := stats[0]
	require.Equal(t, 3, s.Trades)
	require.InDelta(t, 5, s.Volume, 1e-9)
	require.InDelta(t, 100+330+90, s.QuoteVolume, 1e-9)
	require.InDelta(t, 520.0/5, s.VWAP, 1e-9)
	require.InDelta(t, 2, s.BuyVolume, 1e-9)
	require.InDelta(t, 3, s.SellVolume, 1e-9)
	require.InDelta(t, 0.4, s.BuyRatio(), 1e-9)
	require.Equal(t, 90.0, s.Last)
	require.Equal(t, 110.0, s.High)
	require.Equal(t, 90.0, s.Low)
}

// The C# version compared millisecond timestamps with a seconds cut-off, so
// nothing ever left its window.
func TestOldTradesLeaveTheWindow(t *testing.T) {
	w := New(time.Minute, 0)
	w.Add(trade("ETHUSDT", 10, 1, binance.Buy, 0))
	w.Add(trade("ETHUSDT", 11, 1, binance.Buy, 30*time.Second))
	w.Add(trade("ETHUSDT", 12, 1, binance.Buy, 90*time.Second))

	s := w.Snapshot(t0.Add(100 * time.Second))[0]
	require.Equal(t, 1, s.Trades, "only the trade from the last minute stays")
	require.Equal(t, 12.0, s.Last)

	s = w.Snapshot(t0.Add(10 * time.Minute))[0]
	require.Equal(t, 0, s.Trades)
	require.Zero(t, s.VWAP)
}

// The C# version enqueued each trade once per other symbol under its limit,
// so the per-pair limit did not hold.
func TestMaxTradesIsPerSymbol(t *testing.T) {
	w := New(time.Hour, 2)
	for i := 0; i < 5; i++ {
		w.Add(trade("BTCUSDT", float64(100+i), 1, binance.Buy, time.Duration(i)*time.Second))
	}
	w.Add(trade("SOLUSDT", 20, 1, binance.Sell, 0))

	stats := w.Snapshot(t0.Add(time.Minute))
	require.Len(t, stats, 2)
	require.Equal(t, "BTCUSDT", stats[0].Symbol)
	require.Equal(t, 2, stats[0].Trades, "the newest two")
	require.Equal(t, 104.0, stats[0].Last)
	require.Equal(t, 103.0, stats[0].Low)
	require.Equal(t, 1, stats[1].Trades)
}

func TestOutOfOrderTradesAreSorted(t *testing.T) {
	w := New(time.Minute, 0)
	w.Add(trade("XRPUSDT", 2, 1, binance.Buy, 2*time.Second))
	w.Add(trade("XRPUSDT", 1, 1, binance.Buy, time.Second)) // arrives late
	require.Equal(t, 2.0, w.Snapshot(t0.Add(3 * time.Second))[0].Last)
}

func TestConcurrentUse(t *testing.T) {
	w := New(time.Minute, 100)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				w.Add(trade("BTCUSDT", 100, 1, binance.Buy, time.Duration(i)*time.Millisecond))
				_ = w.Snapshot(t0.Add(time.Second))
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 100, w.Snapshot(t0.Add(time.Second))[0].Trades)
}

func TestSumsAreRoundedToBinancePrecision(t *testing.T) {
	w := New(time.Minute, 0)
	for _, q := range []float64{0.1, 0.2, 0.00481, 1.37262} {
		w.Add(trade("BTCUSDT", 84598.05, q, binance.Buy, 0))
	}
	s := w.Snapshot(t0)[0]
	require.Equal(t, 1.67743, s.Volume) // exactly, not 1.6774299999999998
}
