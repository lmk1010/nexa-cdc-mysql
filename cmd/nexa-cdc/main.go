// nexa-cdc: MySQL → MySQL warehouse CDC in Go.
//
//	./nexa-cdc -c config.yaml
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lmk1010/nexa-cdc-mysql/internal/config"
	"github.com/lmk1010/nexa-cdc-mysql/internal/httpsrv"
	"github.com/lmk1010/nexa-cdc-mysql/internal/position"
	"github.com/lmk1010/nexa-cdc-mysql/internal/stats"
	"github.com/lmk1010/nexa-cdc-mysql/internal/syncer"
	"github.com/lmk1010/nexa-cdc-mysql/internal/writer"
)

var (
	configPath = flag.String("c", "config.yaml", "path to config yaml")
	version    = "dev"
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("[boot] nexa-cdc %s starting, config=%s", version, *configPath)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("[boot] config load failed: %v", err)
	}

	promReg := prometheus.NewRegistry()
	st := stats.New(promReg)

	w, err := writer.New(&cfg.Sink, st)
	if err != nil {
		log.Fatalf("[boot] writer init failed: %v", err)
	}
	defer func() { _ = w.Close() }()
	log.Printf("[boot] sink connected: %s:%d/%s", cfg.Sink.Host, cfg.Sink.Port, cfg.Sink.Database)

	ps := position.NewStore(cfg.PositionStore.File)

	sy, err := syncer.New(cfg, w, st, ps)
	if err != nil {
		log.Fatalf("[boot] syncer init failed: %v", err)
	}

	// HTTP endpoints
	httpSrv := httpsrv.New(cfg.HTTP.Addr, st, promReg)
	go func() {
		log.Printf("[boot] http listening on %s", cfg.HTTP.Addr)
		if err := httpSrv.ListenAndServe(); err != nil {
			log.Printf("[http] server exited: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	// SIGTERM / SIGINT 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigCh
		log.Printf("[main] received %s, shutting down", s)
		cancel()
		_ = httpSrv.Close()
	}()

	log.Printf("[boot] tables watched: %v", cfg.Tables.Include)
	if err := sy.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("[main] syncer stopped: %v", err)
	}
	log.Printf("[main] bye")
}
