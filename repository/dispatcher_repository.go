package repository

type DispatcherRepository interface {
	GetCollectionsWithLowQuality() ([]string, error)
	CreateDispatcherPreparedStatements() error
}
