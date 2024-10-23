package config

// DatabaseConfig is the generic structure type for database configuration
type DatabaseConfig struct {
	Type     string `yaml:"type" toml:"Type"`
	Host     string `env:"DB_HOST" yaml:"host" toml:"Host" `
	Port     string `env:"DB_PORT" yaml:"port" toml:"Port"`
	DBName   string `yaml:"db_name" toml:"dbName"`
	User     string `yaml:"user" toml:"User"`
	Password string `env:"DB_PASSWORD" yaml:"password" toml:"Password"`
	SSLMode  string `yaml:"sslmode" toml:"SSLMode"`
	Schema   string `yaml:"schema" toml:"Schema"`
}
