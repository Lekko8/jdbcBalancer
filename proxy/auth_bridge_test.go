package proxy

import (
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestAuthBridge_InvalidClientPassword(t *testing.T) {
	clientConn, proxyClient := net.Pipe()
	proxyBackend, backendConn := net.Pipe()
	defer clientConn.Close()
	defer backendConn.Close()

	dbCfg := DatabaseConfig{
		Login:    "backendUser",
		Pass:     "backendPass",
		DBName:   "backendDB",
		HostPort: "127.0.0.1:5432",
	}

	done := make(chan struct{})

	// Эмулируем бэкенд PostgreSQL (Серверная сторона -> NewBackend)
	go func() {
		defer close(done)
		mockServer := pgproto3.NewBackend(backendConn, backendConn)

		// Читаем Startup от прокси
		_, _ = mockServer.Receive()

		// Запрашиваем аутентификацию SASL (BackendMessage)
		authReq := &pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}}
		mockServer.Send(authReq)
		_ = mockServer.Flush()
	}()

	// Эмулируем клиента (Клиентская сторона -> NewFrontend)
	go func() {
		mockClient := pgproto3.NewFrontend(clientConn, clientConn)

		// Читаем запрос Cleartext от прокси (BackendMessage)
		_, _ = mockClient.Receive()

		// Клиент отсылает заведомо неверный пароль (FrontendMessage)
		badPass := &pgproto3.PasswordMessage{Password: "WRONG_PASSWORD"}
		mockClient.Send(badPass)
		_ = mockClient.Flush()

		// Читаем ответ прокси: приходит ErrorResponse (BackendMessage)
		msg, _ := mockClient.Receive()
		if errResp, ok := msg.(*pgproto3.ErrorResponse); !ok || errResp.Code != "28P01" {
			t.Errorf("expected ErrorResponse with code 28P01, got %+v", msg)
		}
	}()

	err := AuthenticateAndBridge(proxyClient, proxyBackend, map[string]string{}, dbCfg, "CORRECT_CLIENT_PASS")
	if err == nil {
		t.Fatal("expected error on invalid client password, got nil")
	}

	<-done
}

func TestAuthBridge_BackendErrorResponse(t *testing.T) {
	clientConn, proxyClient := net.Pipe()
	proxyBackend, backendConn := net.Pipe()
	defer clientConn.Close()
	defer backendConn.Close()

	dbCfg := DatabaseConfig{
		Login:    "backendUser",
		Pass:     "backendPass",
		DBName:   "backendDB",
		HostPort: "127.0.0.1:5432",
	}

	go func() {
		mockServer := pgproto3.NewBackend(backendConn, backendConn)
		_, _ = mockServer.Receive()
		mockServer.Send(&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "28000",
			Message:  "database system is shutting down",
		})
		_ = mockServer.Flush()
	}()

	go func() {
		mockClient := pgproto3.NewFrontend(clientConn, clientConn)
		_, _ = mockClient.Receive()
	}()

	err := AuthenticateAndBridge(proxyClient, proxyBackend, map[string]string{}, dbCfg, "pass")
	if err == nil {
		t.Fatal("expected error on backend error response, got nil")
	}
}
