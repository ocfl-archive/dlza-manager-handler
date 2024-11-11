package repository

type DispatcherRepository interface {
	GetCollectionsWithLowQuality() ([]string, error)
}
