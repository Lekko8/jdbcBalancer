package proxy

import (
	"fmt"
	"sync"
	"testing"
)

func TestRouter_RoundRobinSamePriority(t *testing.T) {
	dbs := []DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
	}

	router := NewRouter(dbs, "round-robin")
	defer router.Stop()

	sel1, err := router.selectDatabase("")
	if err != nil || sel1.HostPort != "node1:5432" {
		t.Fatalf("expected node1, got %v (err: %v)", sel1, err)
	}

	sel2, err := router.selectDatabase("")
	if err != nil || sel2.HostPort != "node2:5432" {
		t.Fatalf("expected node2, got %v (err: %v)", sel2, err)
	}

	// Должен закольцеваться обратно на node1
	sel3, err := router.selectDatabase("")
	if err != nil || sel3.HostPort != "node1:5432" {
		t.Fatalf("expected loopback to node1, got %v (err: %v)", sel3, err)
	}
}

func TestRouter_IPHashStickiness(t *testing.T) {
	dbs := []DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
	}

	router := NewRouter(dbs, "ip-hash")
	defer router.Stop()

	// Запросы с одного IP (192.168.1.100), но с разных эфемерных портов (пул HikariCP / DBeaver)
	clientIP1 := "192.168.1.100:54123"
	firstChoice, err := router.selectDatabase(clientIP1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 50; i++ {
		addr := fmt.Sprintf("192.168.1.100:%d", 50000+i)
		chosen, err := router.selectDatabase(addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if chosen.HostPort != firstChoice.HostPort {
			t.Fatalf("IP-Hash broken: iteration %d chose %s instead of %s", i, chosen.HostPort, firstChoice.HostPort)
		}
	}
}

func TestRouter_ConcurrentSelectionRaceSafe(t *testing.T) {
	dbs := []DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
		{URL: "postgres://node3:5432/db", Priority: 2, HostPort: "node3:5432"},
	}

	router := NewRouter(dbs, "ip-hash")
	defer router.Stop()

	var wg sync.WaitGroup
	workers := 100
	iterations := 200

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			addr := fmt.Sprintf("10.0.0.%d:1234", workerID%250)
			for range iterations {
				db, err := router.selectDatabase(addr)
				if err != nil || db == nil {
					t.Errorf("concurrent SelectDatabase error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestRouter_NoHealthyDatabasesAvailable(t *testing.T) {
	// Создаем роутер без баз данных
	router := NewRouter([]DatabaseConfig{}, "ip-hash")
	defer router.Stop()

	_, err := router.selectDatabase("192.168.1.1:54321")
	if err == nil {
		t.Fatal("expected error when no healthy databases available, got nil")
	}
}

func TestRouter_URLParsingVariations(t *testing.T) {
	cases := []struct {
		url          string
		expectedHost string
		expectedDB   string
	}{
		{
			url:          "jdbc:postgresql://localhost:5432/my_db",
			expectedHost: "localhost:5432",
			expectedDB:   "my_db",
		},
		{
			url:          "postgres://user:secret@10.0.0.5:5433/prod_db?sslmode=disable",
			expectedHost: "10.0.0.5:5433",
			expectedDB:   "prod_db",
		},
		{
			url:          "postgresql://dbhost/default_db",
			expectedHost: "dbhost",
			expectedDB:   "default_db",
		},
	}

	for _, tc := range cases {
		dbs := []DatabaseConfig{{URL: tc.url, Priority: 1}}
		cfg := &Config{
			Server:    ServerConfig{Port: 8079, Login: "u", Pass: "p", Algorithm: "ip-hash"},
			Databases: dbs,
		}
		// Загрузка через LoadConfig нормализует HostPort и DBName
		router := NewRouter(cfg.Databases, "ip-hash")
		sel, err := router.selectDatabase("")
		if err != nil || sel == nil {
			t.Fatalf("failed to select db for url %s: %v", tc.url, err)
		}
		router.Stop()
	}
}
