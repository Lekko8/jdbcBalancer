package proxy

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestIntegration_FullSessionLifecycle(t *testing.T) {
	clientConn, proxyClient := net.Pipe()
	proxyBackend, mockPG := net.Pipe()
	defer clientConn.Close()
	defer mockPG.Close()

	dbCfg := DatabaseConfig{
		Login:    "pgUser",
		Pass:     "pgPass",
		DBName:   "appDB",
		HostPort: "127.0.0.1:5432",
	}

	// 1. Эмулятор бэкенда PostgreSQL (Серверная сторона -> NewBackend)
	go func() {
		mockServer := pgproto3.NewBackend(mockPG, mockPG)

		// 1. Принимаем StartupMessage от прокси
		_, _ = mockServer.Receive()

		// 2. Отправляем AuthOk (BackendMessage)
		mockServer.Send(&pgproto3.AuthenticationOk{})

		// 3. Отправляем ParameterStatus и ReadyForQuery (BackendMessage)
		mockServer.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
		mockServer.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = mockServer.Flush()
	}()

	// 2. Эмулятор клиента PostgreSQL (Клиентская сторона -> NewFrontend)
	go func() {
		mockClient := pgproto3.NewFrontend(clientConn, clientConn)

		// Получаем AuthOk (BackendMessage)
		msg, err := mockClient.Receive()
		if err != nil {
			t.Errorf("client receive AuthOk error: %v", err)
			return
		}
		if _, ok := msg.(*pgproto3.AuthenticationOk); !ok {
			t.Errorf("expected AuthOk, got %T", msg)
		}

		// Получаем ParameterStatus
		msg, _ = mockClient.Receive()
		if ps, ok := msg.(*pgproto3.ParameterStatus); !ok || ps.Value != "16.0" {
			t.Errorf("expected server_version 16.0, got %+v", msg)
		}

		// Получаем ReadyForQuery
		msg, _ = mockClient.Receive()
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); !ok || rfq.TxStatus != 'I' {
			t.Errorf("expected ReadyForQuery 'I', got %+v", msg)
		}
	}()

	// 3. Запуск тестируемой функции моста авторизации
	done := make(chan error, 1)
	go func() {
		done <- AuthenticateAndBridge(proxyClient, proxyBackend, map[string]string{}, dbCfg, "clientPass")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AuthenticateAndBridge failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("integration test timed out")
	}
}
