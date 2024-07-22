package storage

import (
	"database/sql"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/config"

	_ "github.com/lib/pq"
)

func NewConnection(cfg *config.DatabaseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode)

	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}
	return db, nil
}
