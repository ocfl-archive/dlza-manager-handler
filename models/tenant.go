package models

import "database/sql"

type Tenant struct {
	Id       string         `json:"id"`
	Name     string         `json:"name"`
	Alias    string         `json:"alias"`
	Person   string         `json:"person"`
	Email    string         `json:"email"`
	ApiKeyId sql.NullString `json:"api_key_id"`
}
