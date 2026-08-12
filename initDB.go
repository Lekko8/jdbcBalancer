package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/viper"
)

type Config struct {
	Server    Server           `mapstructure:"server"`
	Databases []DatabaseConfig `mapstructure:"databases"`
}

type Server struct {
	Port              int    `mapstructure:"port"`                // Порт
	Login             string `mapstructure:"login"`               // Логин
	Pass              string `mapstructure:"pass"`                // Пароль
	Timeout           int    `mapstructure:"timeout"`             // Тайм-аут в секундах
	MaxConn           int32    `mapstructure:"max_conn"`            // Максимальное число соединений в пулле (30)
	MinConn           int    `mapstructure:"min_conn"`            // Минимальное число горячих соединений (5)
	MaxConnLifetime   int    `mapstructure:"max_conn_lifetime"`   // Максимальное время жизни коннекта в часах (1)
	MaxConnIdleTime   int    `mapstructure:"max_conn_idle_time"`  // Максимальное время простоя соединения в минутах (30)
	HealthCheckPeriod int    `mapstructure:"health_check_period"` // Частота проверки в минутах (1)
}

type DatabaseConfig struct {
	URL      string `mapstructure:"url"`
	Login    string `mapstructure:"login"`
	Pass     string `mapstructure:"pass"`
	Priority int    `mapstructure:"priority"`
}

func initDB() {

	var config Config
	config.readConfig()

	ctx := context.Background()
	pools := make([]*pgxpool.Pool, len(config.Databases))
	var allPools []*pgxpool.Pool

	for i, db := range config.Databases {

		dsn := buildDSN(db)

		pool, err := config.createPool(ctx, dsn)
		if err != nil {
			log.Printf("Failed to connect to %s: %v", db.URL, err)
			continue
		}
		allPools = append(allPools, pool)

		pools[i] = pool

		if err := checkDB(ctx, pools[i], config.Server.Timeout); err != nil {
			log.Printf("Health check failed for %s: %v", db.URL, err)
		}
	}
	defer func() {
		for _, pool := range allPools {
			pool.Close()
		}
		log.Println("All database pools closed")
	}()

	log.Printf("Connected to %d databases", len(pools))
}

func buildDSN(db DatabaseConfig) string {

	dsn := strings.TrimPrefix(db.URL, "jdbc:postgresql://")

	if !strings.Contains(dsn, "@") {
		// Формат: postgres://login:pass@host:port/database?params ?sslmode=disable
		dsn = fmt.Sprintf("postgres://%s:%s@%s",
			db.Login, db.Pass, dsn)
	}

	return dsn
}

func (c *Config) createPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	config.MaxConns = c.Server.MaxConn
	config.MaxConnLifetime = time.Duration(c.Server.MaxConnLifetime) * time.Hour
	config.MaxConnIdleTime = time.Duration(c.Server.MaxConnIdleTime) * time.Minute
	config.HealthCheckPeriod = time.Duration(c.Server.HealthCheckPeriod) * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (c *Config) readConfig() {

	viper.SetConfigFile("config.yaml")

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&c); err != nil {
		panic(err)
	}

	if len(c.Databases) == 0 {
		panic("В конфиге не указано ни одной базы данных")
	}

	sort.Slice(c.Databases, func(i, j int) bool {
		return c.Databases[i].Priority < c.Databases[j].Priority
	})

}

func checkDB(ctx context.Context, pool *pgxpool.Pool, timeOut int) error {

	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeOut)*time.Second)
	defer cancel()

	var result int
	err := pool.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected result: %d", result)
	}

	return nil
}
