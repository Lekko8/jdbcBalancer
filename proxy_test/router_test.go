package proxy_test

import (
	"fmt"
	"sync"
	"testing"

	"jdbcBalancer/proxy"
)

func TestRouter_RoundRobinSamePriority(t *testing.T) {
	dbs := []proxy.DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
	}

	router := proxy.NewRouter(dbs, "round-robin")
	defer router.Stop()

	sel1, err := router.SelectDatabase("")
	if err != nil || sel1.HostPort != "node1:5432" {
		t.Fatalf("expected node1, got %v (err: %v)", sel1, err)
	}

	sel2, err := router.SelectDatabase("")
	if err != nil || sel2.HostPort != "node2:5432" {
		t.Fatalf("expected node2, got %v (err: %v)", sel2, err)
	}

	// Должен закольцеваться обратно на node1
	sel3, err := router.SelectDatabase("")
	if err != nil || sel3.HostPort != "node1:5432" {
		t.Fatalf("expected loopback to node1, got %v (err: %v)", sel3, err)
	}
}

func TestRouter_IPHashStickiness(t *testing.T) {
	dbs := []proxy.DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
	}

	router := proxy.NewRouter(dbs, "ip-hash")
	defer router.Stop()

	// Запросы с одного IP (192.168.1.100), но с разных эфемерных портов (пул HikariCP / DBeaver)
	clientIP1 := "192.168.1.100:54123"
	firstChoice, err := router.SelectDatabase(clientIP1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 50; i++ {
		addr := fmt.Sprintf("192.168.1.100:%d", 50000+i)
		chosen, err := router.SelectDatabase(addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if chosen.HostPort != firstChoice.HostPort {
			t.Fatalf("IP-Hash broken: iteration %d chose %s instead of %s", i, chosen.HostPort, firstChoice.HostPort)
		}
	}
}

func TestRouter_ConcurrentSelectionRaceSafe(t *testing.T) {
	dbs := []proxy.DatabaseConfig{
		{URL: "postgres://node1:5432/db", Priority: 1, HostPort: "node1:5432"},
		{URL: "postgres://node2:5432/db", Priority: 1, HostPort: "node2:5432"},
		{URL: "postgres://node3:5432/db", Priority: 2, HostPort: "node3:5432"},
	}

	router := proxy.NewRouter(dbs, "ip-hash")
	defer router.Stop()

	var wg sync.WaitGroup
	workers := 100
	iterations := 200

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			addr := fmt.Sprintf("10.0.0.%d:1234", workerID%250)
			for j := 0; j < iterations; j++ {
				db, err := router.SelectDatabase(addr)
				if err != nil || db == nil {
					t.Errorf("concurrent SelectDatabase error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}
