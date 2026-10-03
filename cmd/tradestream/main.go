// Command tradestream follows live Binance trades for a few pairs and shows
// rolling statistics in the terminal and over HTTP.
//
//	tradestream -symbols btcusdt,ethusdt,solusdt -window 1m -http :8090
//
// It uses Binance's public market data only: no API key, no orders.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/skybytescode/binance-trade-stream/binance"
	"github.com/skybytescode/binance-trade-stream/server"
	"github.com/skybytescode/binance-trade-stream/window"
)

func main() {
	symbols := flag.String("symbols", "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt,adausdt", "comma-separated trading pairs")
	windowLength := flag.Duration("window", time.Minute, "length of the rolling window")
	maxTrades := flag.Int("max-trades", 0, "keep at most this many trades per pair in the window (0: no limit)")
	addr := flag.String("http", ":8090", "address for the JSON API and live page (empty: off)")
	table := flag.Bool("table", true, "redraw a table in the terminal every second")
	streamURL := flag.String("url", binance.DefaultURL, "Binance combined-stream URL")
	flag.Parse()

	if err := run(*symbols, *windowLength, *maxTrades, *addr, *table, *streamURL); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(symbols string, windowLength time.Duration, maxTrades int, addr string, table bool, streamURL string) error {
	if windowLength <= 0 {
		return errors.New("-window must be positive")
	}
	if maxTrades < 0 {
		return errors.New("-max-trades must not be negative")
	}
	pairs := strings.Split(symbols, ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// With the table on, logs would scroll it away; keep only warnings.
	level := slog.LevelInfo
	if table {
		level = slog.LevelWarn
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	win := window.New(windowLength, maxTrades)
	trades := make(chan binance.Trade, 1024)
	client := &binance.Client{URL: streamURL, Logger: logger}
	streamErr := make(chan error, 1)
	go func() { streamErr <- client.Stream(ctx, pairs, trades) }()
	go func() {
		for t := range trades {
			win.Add(t)
		}
	}()

	if addr != "" {
		srv := &http.Server{
			Addr:              addr,
			Handler:           server.Handler(win, windowLength, time.Now),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("http server", "err", err)
				stop()
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(shutdownCtx)
		}()
		logger.Info("serving", "addr", addr)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nstopped")
			return nil
		case err := <-streamErr:
			return err
		case now := <-ticker.C:
			if table {
				render(os.Stdout, win.Snapshot(now), windowLength, addr, now)
			}
		}
	}
}

const (
	green = "\033[32m"
	red   = "\033[31m"
	dim   = "\033[2m"
	bold  = "\033[1m"
	reset = "\033[0m"
)

// render redraws the statistics table in place.
func render(w io.Writer, stats []window.Stats, windowLength time.Duration, addr string, now time.Time) {
	fmt.Fprint(w, "\033[H\033[2J") // home + clear
	fmt.Fprintf(w, "%sLive Binance trades%s  rolling %s window  %s%s%s\n\n", bold, reset, windowLength, dim, now.Format("15:04:05"), reset)
	fmt.Fprintf(w, "%-9s %7s %15s %15s %15s %14s  %s\n", "PAIR", "TRADES", "LAST", "VWAP", "HIGH-LOW", "VOLUME", "BUY / SELL")
	for _, s := range stats {
		bar := ratioBar(s.BuyRatio(), 20)
		fmt.Fprintf(w, "%-9s %7d %15s %15s %15s %14.4f  %s %s%3.0f%%%s\n",
			s.Symbol, s.Trades, price(s.Last), price(s.VWAP), price(s.High-s.Low), s.Volume,
			bar, green, 100*s.BuyRatio(), reset)
	}
	if addr != "" {
		fmt.Fprintf(w, "\n%sJSON at http://localhost%s/api/stats · live page at http://localhost%s/ · Ctrl+C to stop%s\n", dim, addr, addr, reset)
	}
}

func ratioBar(buy float64, width int) string {
	n := int(buy*float64(width) + 0.5)
	return green + strings.Repeat("█", n) + red + strings.Repeat("█", width-n) + reset
}

func price(p float64) string {
	switch {
	case p >= 100:
		return fmt.Sprintf("%.2f", p)
	case p >= 1:
		return fmt.Sprintf("%.4f", p)
	default:
		return fmt.Sprintf("%.6f", p)
	}
}
