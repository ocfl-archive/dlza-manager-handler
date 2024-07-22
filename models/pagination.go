package models

type Pagination struct {
	Id             string
	SecondId       string
	Skip           int
	Take           int
	SortDirection  string
	SortKey        string
	AllowedTenants []string
	SearchField    string
}
