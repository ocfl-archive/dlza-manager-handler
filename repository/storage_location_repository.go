package repository

import "github.com/ocfl-archive/dlza-manager-handler/models"

type StorageLocationRepository interface {
	GetStorageLocationsByTenantId(tenantId string) ([]models.StorageLocation, error)
	DeleteStorageLocationById(storageLocationId string) error
	SaveStorageLocation(models.StorageLocation) error
	CreateStorageLocPreparedStatements() error
	GetStorageLocationById(id string) (models.StorageLocation, error)
	GetStorageLocationByObjectInstanceId(id string) (models.StorageLocation, error)
	GetStorageLocationsByObjectId(id string) ([]models.StorageLocation, error)
	GetStorageLocationsByTenantIdPaginated(pagination models.Pagination) ([]models.StorageLocation, int, error)
}
