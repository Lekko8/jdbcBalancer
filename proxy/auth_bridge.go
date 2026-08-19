package proxy

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64*1024)
		return &buf
	},
}

// AuthenticateAndBridge выполняет авторизацию клиента и бэкенда внутри процесса
func AuthenticateAndBridge(
	clientConn net.Conn,
	backendConn net.Conn,
	backendParams map[string]string,
	db DatabaseConfig,
	expectedClientPass string,
) error {
	// 1. Отправляем StartupMessage на бэкенд
	backendStartup := BuildStartupMessage(backendParams, db.Login, db.DBName)
	if _, err := backendConn.Write(backendStartup); err != nil {
		return fmt.Errorf("send startup to backend: %w", err)
	}

	clientBackend := pgproto3.NewBackend(clientConn, clientConn)
	backendFrontend := pgproto3.NewFrontend(backendConn, backendConn)

	backendPassword := db.Pass

	for {
		backendMsg, err := backendFrontend.Receive()
		if err != nil {
			return fmt.Errorf("read from backend: %w", err)
		}

		switch m := backendMsg.(type) {
		case *pgproto3.AuthenticationOk:
			data, _ := m.Encode(nil)
			if _, err := clientConn.Write(data); err != nil {
				return fmt.Errorf("send auth ok to client: %w", err)
			}

		case *pgproto3.AuthenticationSASL:
			if err := handleClientCleartextToBackendSCRAM(clientConn, clientBackend, backendFrontend, db.Login, backendPassword, expectedClientPass); err != nil {
				return err
			}

		case *pgproto3.ErrorResponse:
			data, _ := m.Encode(nil)
			clientConn.Write(data)
			return fmt.Errorf("backend returned error: %s", m.Message)

		case *pgproto3.ReadyForQuery:
			data, _ := m.Encode(nil)
			if _, err := clientConn.Write(data); err != nil {
				return fmt.Errorf("send ReadyForQuery: %w", err)
			}
			return nil

		default:
			data, err := m.Encode(nil)
			if err != nil {
				return fmt.Errorf("encode backend message %T: %w", m, err)
			}
			if _, err := clientConn.Write(data); err != nil {
				return fmt.Errorf("forward message %T: %w", m, err)
			}
		}
	}
}

func handleClientCleartextToBackendSCRAM(
	clientConn net.Conn,
	clientBackend *pgproto3.Backend,
	backendFrontend *pgproto3.Frontend,
	backendUser, backendPass, expectedClientPass string,
) error {
	reqCleartext := &pgproto3.AuthenticationCleartextPassword{}
	reqData, _ := reqCleartext.Encode(nil)
	if _, err := clientConn.Write(reqData); err != nil {
		return fmt.Errorf("request cleartext pass: %w", err)
	}

	clientConn.SetReadDeadline(time.Now().Add(15 * time.Second))
	clientMsg, err := clientBackend.Receive()
	clientConn.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("receive client password: %w", err)
	}

	passMsg, ok := clientMsg.(*pgproto3.PasswordMessage)
	if !ok {
		return fmt.Errorf("expected PasswordMessage, got %T", clientMsg)
	}

	if passMsg.Password != expectedClientPass {
		errResp := &pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "28P01",
			Message:  "password authentication failed for user",
		}
		data, _ := errResp.Encode(nil)
		clientConn.Write(data)
		return fmt.Errorf("invalid client password")
	}

	return executeBackendSCRAM(backendFrontend, backendUser, backendPass)
}

func executeBackendSCRAM(backendFrontend *pgproto3.Frontend, backendUser, backendPass string) error {
	scramClient := NewScramClient(backendUser, backendPass)
	clientFirst := scramClient.ClientFirstMessage()

	backendFrontend.Send(&pgproto3.SASLInitialResponse{
		AuthMechanism: "SCRAM-SHA-256",
		Data:          []byte(clientFirst),
	})
	if err := backendFrontend.Flush(); err != nil {
		return fmt.Errorf("flush SASLInitialResponse: %w", err)
	}

	msg, err := backendFrontend.Receive()
	if err != nil {
		return fmt.Errorf("receive SASL continue: %w", err)
	}
	continueResp, ok := msg.(*pgproto3.AuthenticationSASLContinue)
	if !ok {
		return fmt.Errorf("expected SASLContinue, got %T", msg)
	}

	if err := scramClient.ParseServerFirst(continueResp.Data); err != nil {
		return fmt.Errorf("parse server-first: %w", err)
	}

	clientFinal, err := scramClient.ClientFinalMessage()
	if err != nil {
		return fmt.Errorf("build client-final: %w", err)
	}

	backendFrontend.Send(&pgproto3.SASLResponse{Data: []byte(clientFinal)})
	if err := backendFrontend.Flush(); err != nil {
		return fmt.Errorf("flush SASLResponse: %w", err)
	}

	msg, err = backendFrontend.Receive()
	if err != nil {
		return fmt.Errorf("receive SASL final: %w", err)
	}
	finalResp, ok := msg.(*pgproto3.AuthenticationSASLFinal)
	if !ok {
		return fmt.Errorf("expected SASLFinal, got %T", msg)
	}

	return scramClient.VerifyServerFinal(finalResp.Data)
}

func ProxyBidirectional(conn1, conn2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		bufPtr := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(bufPtr)

		_, _ = io.CopyBuffer(dst, src, *bufPtr)
		if tcpConn, ok := dst.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}

	go pipe(conn1, conn2)
	go pipe(conn2, conn1)

	wg.Wait()
}
