package config

// DatabaseConfig is the generic structure type for database configuration
type DatabaseConfig struct {
	Type     string `yaml:"type" toml:"type"`
	Host     string `env:"DB_HOST" yaml:"host" toml:"host" `
	Port     string `env:"DB_PORT" yaml:"port" toml:"port"`
	DBName   string `yaml:"db_name" toml:"dbname"`
	User     string `yaml:"user" toml:"user"`
	Password string `env:"DB_PASSWORD" yaml:"password" toml:"password"`
	SSLMode  string `yaml:"sslmode" toml:"sslmode"`
	Schema   string `yaml:"schema" toml:"schema"`
}
