package repository

import "github.com/ocfl-archive/dlza-manager-handler/models"

type StoragePartitionRepository interface {
	CreateStoragePartition(partition models.StoragePartition) (string, error)
	DeleteStoragePartition(id string) error
	GetStoragePartitionById(id string) (models.StoragePartition, error)
	UpdateStoragePartition(partition models.StoragePartition) error
	CreateStoragePartitionPreparedStatements() error
	GetStoragePartitionsByLocationIdPaginated(pagination models.Pagination) ([]models.StoragePartition, int, error)
	GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error)
}
