package proxy

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig     `mapstructure:"server"`
	Databases []DatabaseConfig `mapstructure:"databases"`
}

type ServerConfig struct {
	Port       int           `mapstructure:"port"`
	Login      string        `mapstructure:"login"`
	Pass       string        `mapstructure:"pass"`
	Database   string        `mapstructure:"database"`
	Algorithm  string        `mapstructure:"algorithm"` // "ip-hash" или "round-robin"
	TimeoutSec int           `mapstructure:"timeout_sec"`
	Timeout    time.Duration `mapstructure:"-"`
}

type DatabaseConfig struct {
	URL      string `mapstructure:"url"`
	Login    string `mapstructure:"login"`
	Pass     string `mapstructure:"pass"`
	CredsCmd string `mapstructure:"creds_cmd"`
	Priority int    `mapstructure:"priority"`

	HostPort string `mapstructure:"-"` // сетевой адрес
	DBName   string `mapstructure:"-"`
}

// LoadConfig читает конфиг файл и заполняет Config
func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
	}
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if len(cfg.Databases) == 0 {
		return nil, fmt.Errorf("no databases configured")
	}

	if cfg.Server.Algorithm == "" {
		cfg.Server.Algorithm = "ip-hash"
	} else {
		cfg.Server.Algorithm = strings.ToLower(cfg.Server.Algorithm)
	}

	if cfg.Server.TimeoutSec <= 0 {
		cfg.Server.Timeout = 5 * time.Second
	} else {
		cfg.Server.Timeout = time.Duration(cfg.Server.TimeoutSec) * time.Second
	}

	for i := range cfg.Databases {
		cfg.Databases[i].HostPort = extractHostPort(cfg.Databases[i].URL)
		cfg.Databases[i].DBName = extractDBName(cfg.Databases[i].URL)
	}

	sort.Slice(cfg.Databases, func(i, j int) bool {
		return cfg.Databases[i].Priority < cfg.Databases[j].Priority
	})

	return &cfg, nil
}
