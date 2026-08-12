package main

import (
	"context"
	"net"
)

type PGResolver interface {
	GetPGConn(ctx context.Context, clientAddr net.Addr, parameters map[string]string) (net.Conn, error)
}

type MyResolver struct {
	// Здесь можно хранить конфигурацию или пулы соединений
	databases []DatabaseConfig
}

func initPgBroker() {
	// TODO: приём подключений в горутинах и открытие подключений в бд
	// TODO: обработка запросов через ещё один уровень горутин
}
