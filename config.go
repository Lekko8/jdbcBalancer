package main

import (
	"sort"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/viper"
)

type Config struct {
	Server    Server           `mapstructure:"server"`
	Databases []DatabaseConfig `mapstructure:"databases"`
}

type Server struct {
	Port    int    `mapstructure:"port"`    // Порт
	Login   string `mapstructure:"login"`   // Логин
	Pass    string `mapstructure:"pass"`    // Пароль
	Timeout int    `mapstructure:"timeout"` // Тайм-аут в секундах
}

type DatabaseConfig struct {
	URL      string `mapstructure:"url"`
	Login    string `mapstructure:"login"`
	Pass     string `mapstructure:"pass"`
	Priority int    `mapstructure:"priority"`
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
		panic("No databases in config")
	}

	sort.Slice(c.Databases, func(i, j int) bool {
		return c.Databases[i].Priority < c.Databases[j].Priority
	})

}

func buildDSN(db DatabaseConfig) string {
	dsn := strings.TrimPrefix(db.URL, "jdbc:postgresql://")

	if !strings.Contains(dsn, "@") {
		// Формат: postgres://login:pass@host:port/database?params ?sslmode=disable
		dsn = "postgres://" + db.Login + ":" + db.Pass + "@" + dsn
	}

	if !strings.Contains(dsn, "sslmode") {
		if strings.Contains(dsn, "?") {
			dsn = dsn + "&sslmode=disable"
		} else {
			dsn = dsn + "?sslmode=disable"
		}
	}

	return dsn
}
