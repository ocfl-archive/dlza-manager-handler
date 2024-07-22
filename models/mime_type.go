package models

import "database/sql"

type MimeType struct {
	Id        sql.NullString
	FileCount int
}
