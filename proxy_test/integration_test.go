package proxy_test

import (
	"net"
	"testing"
	"time"

	"jdbcBalancer/proxy"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestIntegration_FullSessionLifecycle(t *testing.T) {
	clientConn, proxyClient := net.Pipe()
	proxyBackend, mockPG := net.Pipe()
	defer clientConn.Close()
	defer mockPG.Close()

	dbCfg := proxy.DatabaseConfig{
		Login:    "pgUser",
		Pass:     "pgPass",
		DBName:   "appDB",
		HostPort: "127.0.0.1:5432",
	}

	go func() {
		fe := pgproto3.NewFrontend(mockPG, mockPG)
		_, _ = fe.Receive()

		authOk := &pgproto3.AuthenticationOk{}
		data, _ := authOk.Encode(nil)
		_, _ = mockPG.Write(data)

		param := &pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"}
		pData, _ := param.Encode(nil)
		_, _ = mockPG.Write(pData)

		rfq := &pgproto3.ReadyForQuery{TxStatus: 'I'}
		rData, _ := rfq.Encode(nil)
		_, _ = mockPG.Write(rData)
	}()

	// Запуск эмулятора бэкенда PostgreSQL (Серверная сторона -> NewBackend)
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

	// Эмулятор клиента PostgreSQL (Клиентская сторона -> NewFrontend)
	go func() {
		mockClient := pgproto3.NewFrontend(clientConn, clientConn)

		// 1. Получаем AuthOk (BackendMessage)
		msg, err := mockClient.Receive()
		if err != nil {
			t.Errorf("client receive AuthOk error: %v", err)
			return
		}
		if _, ok := msg.(*pgproto3.AuthenticationOk); !ok {
			t.Errorf("expected AuthenticationOk, got %T", msg)
		}

		// 2. Получаем ParameterStatus
		msg, _ = mockClient.Receive()
		if ps, ok := msg.(*pgproto3.ParameterStatus); !ok || ps.Value != "16.0" {
			t.Errorf("expected server_version 16.0, got %+v", msg)
		}

		// 3. Получаем ReadyForQuery
		msg, _ = mockClient.Receive()
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); !ok || rfq.TxStatus != 'I' {
			t.Errorf("expected ReadyForQuery 'I', got %+v", msg)
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- proxy.AuthenticateAndBridge(proxyClient, proxyBackend, map[string]string{}, dbCfg, "clientPass")
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
