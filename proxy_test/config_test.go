package proxy_test

import (
	"os"
	"testing"

	"jdbcBalancer/proxy"
)

func TestConfig_LoadValidYAML(t *testing.T) {
	yamlContent := `
server:
  port: 9090
  login: "admin"
  pass: "secret"
  database: "app_db"
  timeout_sec: 10
databases:
  - url: "jdbc:postgresql://10.0.0.1:5432/db_secondary?sslmode=disable"
    login: "u2"
    pass: "p2"
    priority: 2
  - url: "postgres://u1:p1@10.0.0.2:5432/db_primary"
    login: "u1"
    pass: "p1"
    priority: 1
`
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(yamlContent)); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close()

	cfg, err := proxy.LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.Port)
	}

	if cfg.Databases[0].Priority != 1 || cfg.Databases[0].HostPort != "10.0.0.2:5432" {
		t.Errorf("expected primary database with priority 1 first, got %+v", cfg.Databases[0])
	}
	if cfg.Databases[0].DBName != "db_primary" {
		t.Errorf("expected dbName 'db_primary', got '%s'", cfg.Databases[0].DBName)
	}

	if cfg.Databases[1].Priority != 2 || cfg.Databases[1].HostPort != "10.0.0.1:5432" {
		t.Errorf("expected secondary database with priority 2, got %+v", cfg.Databases[1])
	}
	if cfg.Databases[1].DBName != "db_secondary" {
		t.Errorf("expected dbName 'db_secondary', got '%s'", cfg.Databases[1].DBName)
	}
}

func TestConfig_EmptyDatabasesError(t *testing.T) {
	yamlContent := `
server:
  port: 8079
  login: "test"
  pass: "pass"
databases: []
`
	tmpFile, err := os.CreateTemp("", "config-empty-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	_, _ = tmpFile.Write([]byte(yamlContent))
	_ = tmpFile.Close()

	_, err = proxy.LoadConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("expected error on empty databases list, got nil")
	}
}
