package service

type StorageLocationService interface {
	GetStorageLocationsStatusForCollectionAlias(id string, size int64, signature string, head string) (string, error)
}
