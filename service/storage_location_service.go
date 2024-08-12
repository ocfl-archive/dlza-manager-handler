package service

type StorageLocationService interface {
	GetStorageLocationsStatusForCollectionAlias(id string, size int64) (string, error)
}
