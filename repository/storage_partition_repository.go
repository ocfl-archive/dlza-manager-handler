package repository

import "github.com/ocfl-archive/dlza-manager/models"

type StoragePartitionRepository interface {
	CreateStoragePartition(partition models.StoragePartition) (string, error)
	DeleteStoragePartitionById(id string) error
	GetStoragePartitionById(id string) (models.StoragePartition, error)
	UpdateStoragePartition(partition models.StoragePartition) error
	GetStoragePartitionsByLocationIdPaginated(pagination models.Pagination) ([]models.StoragePartition, int, error)
	GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error)
	GetStoragePartitionByObjectSignatureAndLocation(signature string, locationId string) (models.StoragePartition, error)
}
