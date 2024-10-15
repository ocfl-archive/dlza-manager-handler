package config

import (
	"github.com/jinzhu/configor"
	"log"
	"os"
)

type Service struct {
	ServiceName string         `yaml:"service_name" toml:"ServiceName"`
	Host        string         `yaml:"host" toml:"Host"`
	Port        int            `yaml:"port" toml:"Port"`
	Database    DatabaseConfig `yaml:"database" toml:"Database"`
}

type Logging struct {
	LogLevel string
	LogFile  string
}

type Config struct {
	Handler        Service `yaml:"handler" toml:"Handler"`
	StorageHandler Service `yaml:"storage-handler" toml:"StorageHandler"`
	Clerk          Service `yaml:"clerk" toml:"Clerk"`
	Logging        Logging `yaml:"logging" toml:"Logging"`
}

// GetConfig creates a new config from a given environment
func GetConfig(configFile string) Config {
	conf := Config{}
	if configFile == "" {
		configFile = "config.yml"
	}
	err := configor.Load(&conf, configFile)
	if err != nil {
		log.Fatal(err)
	}
	if conf.Handler.Database.Password == "" {
		conf.Handler.Database.Password = os.Getenv("DB_PASSWORD")
	}
	return conf
}
