package proxy_test

import (
	"encoding/binary"
	"net"
	"testing"

	"jdbcBalancer/proxy"
)

func TestProtocol_SSLAndGSSRejection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		// 1. Клиент шлет GSSENCRequest (код 80877104)
		gssReq := make([]byte, 8)
		binary.BigEndian.PutUint32(gssReq[:4], 8)
		binary.BigEndian.PutUint32(gssReq[4:], proxy.GSSENCRequestCode)
		_, _ = clientConn.Write(gssReq)

		respGSS := make([]byte, 1)
		_, _ = clientConn.Read(respGSS)
		if respGSS[0] != 'N' {
			t.Errorf("expected 'N' for GSSENC rejection, got %c", respGSS[0])
		}

		// 2. Клиент шлет SSLRequest (код 80877103)
		sslReq := make([]byte, 8)
		binary.BigEndian.PutUint32(sslReq[:4], 8)
		binary.BigEndian.PutUint32(sslReq[4:], proxy.SSLRequestCode)
		_, _ = clientConn.Write(sslReq)

		respSSL := make([]byte, 1)
		_, _ = clientConn.Read(respSSL)
		if respSSL[0] != 'N' {
			t.Errorf("expected 'N' for SSL rejection, got %c", respSSL[0])
		}

		// 3. Клиент шлет обычный StartupMessage v3.0
		startup := proxy.BuildStartupMessage(map[string]string{
			"application_name": "GoTestApp",
		}, "clientUser", "clientDB")
		_, _ = clientConn.Write(startup)
	}()

	_, params, err := proxy.ReadStartupPacket(serverConn)
	if err != nil {
		t.Fatalf("ReadStartupPacket failed: %v", err)
	}

	if params["user"] != "clientUser" {
		t.Errorf("expected user 'clientUser', got '%s'", params["user"])
	}
	if params["database"] != "clientDB" {
		t.Errorf("expected database 'clientDB', got '%s'", params["database"])
	}
	if params["application_name"] != "GoTestApp" {
		t.Errorf("expected application_name 'GoTestApp', got '%s'", params["application_name"])
	}
}

func TestProtocol_BuildStartupMessage(t *testing.T) {
	initialParams := map[string]string{
		"user":             "oldUser",
		"database":         "oldDB",
		"application_name": "balancer",
	}

	packet := proxy.BuildStartupMessage(initialParams, "newBackendUser", "newBackendDB")
	if len(packet) < 8 {
		t.Fatalf("packet too short: %d", len(packet))
	}

	length := binary.BigEndian.Uint32(packet[:4])
	if int(length) != len(packet) {
		t.Fatalf("packet length header mismatch: header=%d, actual=%d", length, len(packet))
	}

	code := binary.BigEndian.Uint32(packet[4:8])
	if code != proxy.ProtocolVersion30 {
		t.Fatalf("expected protocol v3.0 (196608), got %d", code)
	}
}
