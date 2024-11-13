package repository

type DispatcherRepository interface {
	GetLowQualityCollectionsWithObjectIds() (map[string][]string, error)
}
