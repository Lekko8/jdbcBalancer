package proxy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

type ScramClient struct {
	username        string
	password        string
	clientNonce     string
	serverNonce     string
	salt            []byte
	iterations      int
	saltedPassword  []byte
	authMessage     string
	clientProof     []byte
	serverSignature []byte
	state           string
}

func NewScramClient(username, password string) *ScramClient {
	return &ScramClient{
		username:    username,
		password:    password,
		clientNonce: generateSecureNonce(),
		state:       "start",
	}
}

func generateSecureNonce() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return base64.RawStdEncoding.EncodeToString(b)
}

func (sc *ScramClient) ClientFirstMessage() string {
	if sc.state != "start" {
		return ""
	}
	sc.state = "first_sent"
	return fmt.Sprintf("n,,n=%s,r=%s", sc.username, sc.clientNonce)
}

func (sc *ScramClient) ParseServerFirst(data []byte) error {
	if sc.state != "first_sent" {
		return fmt.Errorf("invalid state: expected first_sent, got %s", sc.state)
	}

	parts := strings.Split(string(data), ",")
	var r, s string
	var iter int

	for _, part := range parts {
		if strings.HasPrefix(part, "r=") {
			r = part[2:]
		} else if strings.HasPrefix(part, "s=") {
			s = part[2:]
		} else if strings.HasPrefix(part, "i=") {
			val, err := strconv.Atoi(part[2:])
			if err != nil {
				return fmt.Errorf("invalid iteration count: %w", err)
			}
			iter = val
		}
	}

	if r == "" || s == "" || iter <= 0 {
		return fmt.Errorf("malformed server-first-message: %s", string(data))
	}

	if !strings.HasPrefix(r, sc.clientNonce) {
		return fmt.Errorf("server nonce mismatch: must start with client nonce")
	}

	sc.serverNonce = r
	saltBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("decode salt base64: %w", err)
	}
	sc.salt = saltBytes
	sc.iterations = iter

	sc.saltedPassword = pbkdf2.Key([]byte(sc.password), sc.salt, sc.iterations, 32, sha256.New)

	clientFirstBare := fmt.Sprintf("n=%s,r=%s", sc.username, sc.clientNonce)
	sc.authMessage = clientFirstBare + "," + string(data)
	sc.state = "server_parsed"
	return nil
}

func (sc *ScramClient) ClientFinalMessage() (string, error) {
	if sc.state != "server_parsed" {
		return "", fmt.Errorf("invalid state: expected server_parsed, got %s", sc.state)
	}

	clientFinalWithoutProof := fmt.Sprintf("c=biws,r=%s", sc.serverNonce)
	sc.authMessage = sc.authMessage + "," + clientFinalWithoutProof

	clientKey := hmacSHA256(sc.saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(sc.authMessage))

	sc.clientProof = make([]byte, len(clientKey))
	for i := range clientKey {
		sc.clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	serverKey := hmacSHA256(sc.saltedPassword, []byte("Server Key"))
	sc.serverSignature = hmacSHA256(serverKey, []byte(sc.authMessage))

	proof := base64.StdEncoding.EncodeToString(sc.clientProof)
	sc.state = "final_sent"
	return clientFinalWithoutProof + ",p=" + proof, nil
}

func (sc *ScramClient) VerifyServerFinal(data []byte) error {
	if sc.state != "final_sent" {
		return fmt.Errorf("invalid state: expected final_sent, got %s", sc.state)
	}

	parts := strings.Split(string(data), ",")
	for _, part := range parts {
		if strings.HasPrefix(part, "v=") {
			expectedSig := part[2:]
			actualSig := base64.StdEncoding.EncodeToString(sc.serverSignature)
			if actualSig != expectedSig {
				return fmt.Errorf("server signature mismatch: expected %s, got %s", expectedSig, actualSig)
			}
			sc.state = "done"
			return nil
		} else if strings.HasPrefix(part, "e=") {
			return fmt.Errorf("server reported error: %s", part[2:])
		}
	}
	return fmt.Errorf("missing verifier v= in server-final-message")
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
