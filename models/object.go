package models

import "database/sql"

type Object struct {
	Signature         string         `json:"signature"`
	Sets              []string       `json:"sets"`
	Identifiers       []string       `json:"identifiers"`
	Title             string         `json:"title"`
	AlternativeTitles []string       `json:"alternative_titles"`
	Description       string         `json:"description"`
	Keywords          []string       `json:"keywords"`
	References        []string       `json:"references"`
	IngestWorkflow    string         `json:"ingest_workflow"`
	User              string         `json:"user"`
	Address           string         `json:"address"`
	Created           string         `json:"created"`
	LastChanged       string         `json:"last_changed"`
	Size              int64          `json:"size"`
	Id                string         `json:"id"`
	CollectionId      string         `json:"collection_id"`
	Checksum          string         `json:"checksum"`
	Collection        string         `json:"collection"`
	Holding           sql.NullString `json:"holding"`
	Authors           []string       `json:"authors"`
	Expiration        sql.NullString `json:"expiration"`
	Head              string         `json:"head"`
	Versions          string         `json:"versions"`
	//virtual columns
	TotalFileSize  sql.NullInt64 `json:"totalFileSize"`
	TotalFileCount sql.NullInt64 `json:"totalFileCount"`
}