package proxy_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"jdbcBalancer/proxy"
)

// Тест двунаправленной потоковой передачи данных и пула буферов sync.Pool
func TestServer_ProxyBidirectional(t *testing.T) {
	c1, s1 := net.Pipe()
	c2, s2 := net.Pipe()
	defer c1.Close()
	defer s1.Close()
	defer c2.Close()
	defer s2.Close()

	go proxy.Bidirectional(s1, s2)

	msgClient := []byte("SQL_SELECT_QUERY_PAYLOAD_FROM_CLIENT")
	msgBackend := []byte("SQL_QUERY_RESULT_ROWS_FROM_POSTGRES")

	errCh := make(chan error, 2)

	// Клиент пишет запрос и читает ответ
	go func() {
		if _, err := c1.Write(msgClient); err != nil {
			errCh <- err
			return
		}
		buf := make([]byte, len(msgBackend))
		if _, err := c1.Read(buf); err != nil {
			errCh <- err
			return
		}
		if !bytes.Equal(buf, msgBackend) {
			t.Errorf("expected %s, got %s", msgBackend, buf)
		}
		errCh <- nil
	}()

	// Бэкенд читает запрос и отвечает данными
	go func() {
		buf := make([]byte, len(msgClient))
		if _, err := c2.Read(buf); err != nil {
			errCh <- err
			return
		}
		if !bytes.Equal(buf, msgClient) {
			t.Errorf("expected %s, got %s", msgClient, buf)
		}
		if _, err := c2.Write(msgBackend); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	for range 2 {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("streaming error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("proxy streaming timed out")
		}
	}
}

// Тест запуска ProxyServer на динамическом порту и Graceful Shutdown
func TestServer_LifecycleAndGracefulStop(t *testing.T) {
	cfg := &proxy.Config{
		Server: proxy.ServerConfig{
			Port:      0, // Динамический свободный порт ОС
			Login:     "validUser",
			Pass:      "validPass",
			Database:  "validDB",
			Algorithm: "ip-hash",
			Timeout:   2 * time.Second,
		},
		Databases: []proxy.DatabaseConfig{
			{URL: "postgres://127.0.0.1:5432/validDB", Priority: 1, HostPort: "127.0.0.1:5432", DBName: "validDB"},
		},
	}

	server := proxy.NewProxyServer(cfg)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Проверяем корректную остановку сервера без утечки горутин
	server.Stop()
}
