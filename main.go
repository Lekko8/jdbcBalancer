package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jdbcBalancer/proxy"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to YAML configuration file")
	logJSON    = flag.Bool("json-log", false, "Enable structured JSON logging format")
	logLevel   = flag.String("log-level", "info", "Log level: debug, info, warn, error")
)

func initLogger(isJSON bool, levelStr string) {
	var lvl slog.Level
	switch levelStr {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: lvl,
	}

	var handler slog.Handler
	if isJSON {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
}

func main() {
	flag.Parse()

	// Инициализация логирования
	initLogger(*logJSON, *logLevel)
	slog.Info("Starting jdbcBalancer PostgreSQL Proxy...", "version", "2.0-inprocess")

	// Чтение конфигурации
	cfg, err := proxy.LoadConfig(*configPath)
	if err != nil {
		slog.Error("Fatal: failed to load configuration", "path", *configPath, "err", err)
		os.Exit(1)
	}

	slog.Info("Configuration loaded successfully",
		"listen_port", cfg.Server.Port,
		"server_login", cfg.Server.Login,
		"target_database", cfg.Server.Database,
		"configured_backends", len(cfg.Databases),
	)

	// Запуск прокси-сервера
	server := proxy.NewProxyServer(cfg)
	if err := server.Start(); err != nil {
		slog.Error("Fatal: failed to start proxy server", "port", cfg.Server.Port, "err", err)
		os.Exit(1)
	}

	slog.Info("Proxy server running in-process",
		"address", fmt.Sprintf("0.0.0.0:%d", cfg.Server.Port),
	)

	// Graceful shutdown
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-stopSignal
	slog.Info("Received shutdown signal", "signal", sig.String())

	// 15 секунд на завершение активных клиентских сессий
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		server.Stop()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("Proxy server gracefully stopped without errors")
	case <-shutdownCtx.Done():
		slog.Warn("Graceful shutdown timeout exceeded, forcing exit")
	}

	slog.Info("jdbcBalancer terminated")
}
