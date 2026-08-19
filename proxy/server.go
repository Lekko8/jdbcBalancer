package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type ProxyServer struct {
	cfg      *Config
	router   *Router
	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewProxyServer(cfg *Config) *ProxyServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProxyServer{
		cfg:    cfg,
		router: NewRouter(cfg.Databases, cfg.Server.Algorithm),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *ProxyServer) Start() error {
	addr := fmt.Sprintf(":%d", p.cfg.Server.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	p.listener = l

	slog.Info("Proxy server started", "port", p.cfg.Server.Port, "backends", len(p.cfg.Databases), "algorithm", p.cfg.Server.Algorithm)

	p.wg.Add(1)
	go p.acceptLoop()

	return nil
}

func (p *ProxyServer) Stop() {
	p.cancel()
	if p.listener != nil {
		p.listener.Close()
	}
	p.router.Stop()
	p.wg.Wait()
	slog.Info("Proxy server gracefully stopped")
}

func (p *ProxyServer) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				slog.Error("Accept error", "err", err)
				continue
			}
		}

		p.wg.Add(1)
		go func(c net.Conn) {
			defer p.wg.Done()
			defer c.Close()
			p.handleConnection(c)
		}(conn)
	}
}

func (p *ProxyServer) handleConnection(clientConn net.Conn) {
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	clientAddr := clientConn.RemoteAddr().String()

	// 1. Читаем Startup-пакет
	_, clientParams, err := ReadStartupPacket(clientConn)
	if err != nil {
		slog.Warn("Failed to read startup packet", "client", clientAddr, "err", err)
		return
	}

	// 2. Валидация логина и имени базы
	if clientParams["user"] != p.cfg.Server.Login {
		slog.Warn("Auth failed: invalid user", "client", clientAddr, "user", clientParams["user"])
		sendFatalError(clientConn, "28000", fmt.Sprintf("role \"%s\" does not exist", clientParams["user"]))
		return
	}

	if p.cfg.Server.Database != "" && clientParams["database"] != p.cfg.Server.Database {
		slog.Warn("Auth failed: invalid database", "client", clientAddr, "db", clientParams["database"])
		sendFatalError(clientConn, "3D000", fmt.Sprintf("database \"%s\" does not exist", clientParams["database"]))
		return
	}

	// 3. Выбор целевой БД через роутер с поддержкой IP-Hash / Sticky Sessions
	targetDB, err := p.router.SelectDatabase(clientAddr)
	if err != nil {
		slog.Error("No healthy backend available", "client", clientAddr, "err", err)
		sendFatalError(clientConn, "57P03", "cannot connect to upstream database: all backends unhealthy")
		return
	}

	// 4. Прямое подключение к бэкенду
	backendConn, err := net.DialTimeout("tcp", targetDB.HostPort, p.cfg.Server.Timeout)
	if err != nil {
		slog.Error("Failed to dial backend", "backend", targetDB.HostPort, "err", err)
		sendFatalError(clientConn, "08006", "failed to connect to upstream server")
		return
	}
	defer backendConn.Close()

	if tcpConn, ok := backendConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	// 5. Внутрипроцессная аутентификация
	err = AuthenticateAndBridge(clientConn, backendConn, clientParams, *targetDB, p.cfg.Server.Pass)
	if err != nil {
		slog.Error("Authentication bridging failed", "client", clientAddr, "backend", targetDB.HostPort, "err", err)
		return
	}

	// 6. Прозрачный стриминг данных
	ProxyBidirectional(clientConn, backendConn)
}

func sendFatalError(conn net.Conn, code, msg string) {
	errResp := &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     code,
		Message:  msg,
	}
	if data, err := errResp.Encode(nil); err == nil {
		_, _ = conn.Write(data)
	}
}
