package models

import "database/sql"

type Collection struct {
	Id          string `json:"id"`
	Alias       string `json:"alias"`
	Description string `json:"description"`
	Owner       string `json:"owner"`
	OwnerMail   string `json:"ownerMail"`
	Name        string `json:"name"`
	Quality     int    `json:"quality"`
	TenantId    string `json:"tenantId"`
	//virtual columns
	TotalFileSize    sql.NullInt64 `json:"totalFileSize"`
	TotalFileCount   sql.NullInt32 `json:"totalFileCount"`
	TotalObjectCount sql.NullInt32 `json:"totalObjectCount"`
}
