package proxy

import (
	"strings"
	"testing"
)

func TestScram_ClientFirstMessage(t *testing.T) {
	client := NewScramClient("user", "pencil")
	first := client.clientFirstMessage()

	if !strings.HasPrefix(first, "n,,n=user,r=") {
		t.Fatalf("expected client-first format 'n,,n=user,r=...', got %s", first)
	}

	if second := client.clientFirstMessage(); second != "" {
		t.Fatalf("expected empty string on repeat ClientFirstMessage, got %s", second)
	}
}

func TestScram_ParseServerFirst_Valid(t *testing.T) {
	client := NewScramClient("user", "pencil")
	first := client.clientFirstMessage()

	clientNonce := strings.TrimPrefix(first, "n,,n=user,r=")
	serverNonce := clientNonce + "SERVERNONCE123"
	serverFirstPayload := "r=" + serverNonce + ",s=c2FsdHNhbHQ=,i=4096"

	err := client.parseServerFirst([]byte(serverFirstPayload))
	if err != nil {
		t.Fatalf("unexpected error parsing server first: %v", err)
	}

	finalMsg, err := client.clientFinalMessage()
	if err != nil {
		t.Fatalf("failed to build client final message: %v", err)
	}

	if !strings.HasPrefix(finalMsg, "c=biws,r="+serverNonce+",p=") {
		t.Fatalf("invalid client final message format: %s", finalMsg)
	}
}

func TestScram_ParseServerFirst_NonceMismatch(t *testing.T) {
	client := NewScramClient("user", "pencil")
	_ = client.clientFirstMessage()

	serverFirstPayload := "r=WRONGNONCE123,s=c2FsdHNhbHQ=,i=4096"
	err := client.parseServerFirst([]byte(serverFirstPayload))
	if err == nil {
		t.Fatal("expected error on server nonce mismatch, got nil")
	}
}

func TestScram_VerifyServerFinal_Mismatch(t *testing.T) {
	client := NewScramClient("user", "pencil")
	first := client.clientFirstMessage()
	clientNonce := strings.TrimPrefix(first, "n,,n=user,r=")
	serverNonce := clientNonce + "12345"

	_ = client.parseServerFirst([]byte("r=" + serverNonce + ",s=c2FsdHNhbHQ=,i=4096"))
	_, _ = client.clientFinalMessage()

	err := client.verifyServerFinal([]byte("v=INVALID_BASE64_SIGNATURE=="))
	if err == nil {
		t.Fatal("expected signature mismatch error, got nil")
	}
}
