package models

import "database/sql"

type Pronom struct {
	Id        sql.NullString
	FileCount int
}
