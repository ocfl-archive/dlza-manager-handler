package repository

import "github.com/ocfl-archive/dlza-manager/models"

type StorageLocationRepository interface {
	GetAllStorageLocations() ([]models.StorageLocation, error)
	GetStorageLocationsByTenantId(tenantId string) ([]models.StorageLocation, error)
	DeleteStorageLocationById(storageLocationId string) error
	SaveStorageLocation(models.StorageLocation) (string, error)
	UpdateStorageLocation(models.StorageLocation) error
	GetStorageLocationById(id string) (models.StorageLocation, error)
	GetStorageLocationByObjectInstanceId(id string) (models.StorageLocation, error)
	GetStorageLocationsByObjectId(id string) ([]models.StorageLocation, error)
	GetAmountOfErrorsForStorageLocationId(id string) (int, error)
	GetAmountOfObjectsForStorageLocationId(id string) (int, error)
	GetStorageLocationsByTenantOrCollectionIdPaginated(pagination models.Pagination) ([]models.StorageLocation, int, error)
}
