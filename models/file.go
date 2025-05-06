package models

import "database/sql"

type File struct {
	Checksum string        `json:"checksum"`
	Name     []string      `json:"name"`
	Size     int           `json:"size"`
	MimeType string        `json:"mime_type"`
	Pronom   string        `json:"pronom"`
	Width    sql.NullInt64 `json:"width"`
	Height   sql.NullInt64 `json:"height"`
	Duration sql.NullInt64 `json:"duration"`
	Id       string        `json:"id"`
	ObjectId string        `json:"object_id"`
}
