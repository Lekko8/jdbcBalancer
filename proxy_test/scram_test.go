package proxy_test

import (
	"strings"
	"testing"

	"jdbcBalancer/proxy"
)

func TestScram_ClientFirstMessage(t *testing.T) {
	client := proxy.NewScramClient("user", "pencil")
	first := client.ClientFirstMessage()

	if !strings.HasPrefix(first, "n,,n=user,r=") {
		t.Fatalf("expected client-first format 'n,,n=user,r=...', got %s", first)
	}

	if second := client.ClientFirstMessage(); second != "" {
		t.Fatalf("expected empty string on repeat ClientFirstMessage, got %s", second)
	}
}

func TestScram_ParseServerFirst_Valid(t *testing.T) {
	client := proxy.NewScramClient("user", "pencil")
	first := client.ClientFirstMessage()

	clientNonce := strings.TrimPrefix(first, "n,,n=user,r=")
	serverNonce := clientNonce + "SERVERNONCE123"
	serverFirstPayload := "r=" + serverNonce + ",s=c2FsdHNhbHQ=,i=4096"

	err := client.ParseServerFirst([]byte(serverFirstPayload))
	if err != nil {
		t.Fatalf("unexpected error parsing server first: %v", err)
	}

	finalMsg, err := client.ClientFinalMessage()
	if err != nil {
		t.Fatalf("failed to build client final message: %v", err)
	}

	if !strings.HasPrefix(finalMsg, "c=biws,r="+serverNonce+",p=") {
		t.Fatalf("invalid client final message format: %s", finalMsg)
	}
}

func TestScram_ParseServerFirst_NonceMismatch(t *testing.T) {
	client := proxy.NewScramClient("user", "pencil")
	_ = client.ClientFirstMessage()

	serverFirstPayload := "r=WRONGNONCE123,s=c2FsdHNhbHQ=,i=4096"
	err := client.ParseServerFirst([]byte(serverFirstPayload))
	if err == nil {
		t.Fatal("expected error on server nonce mismatch, got nil")
	}
}

func TestScram_VerifyServerFinal_Mismatch(t *testing.T) {
	client := proxy.NewScramClient("user", "pencil")
	first := client.ClientFirstMessage()
	clientNonce := strings.TrimPrefix(first, "n,,n=user,r=")
	serverNonce := clientNonce + "12345"

	_ = client.ParseServerFirst([]byte("r=" + serverNonce + ",s=c2FsdHNhbHQ=,i=4096"))
	_, _ = client.ClientFinalMessage()

	err := client.VerifyServerFinal([]byte("v=INVALID_BASE64_SIGNATURE=="))
	if err == nil {
		t.Fatal("expected signature mismatch error, got nil")
	}
}
