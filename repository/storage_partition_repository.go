package repository

import "github.com/ocfl-archive/dlza-manager/models"

type StoragePartitionRepository interface {
	CreateStoragePartition(partition models.StoragePartition) (string, error)
	CreateStoragePartitionGroupElement(partitionGroupElement models.StoragePartitionGroup) (string, error)
	DeleteStoragePartitionGroupElementByStoragePartitionId(id string) error
	DeleteStoragePartitionById(id string) error
	GetStoragePartitionById(id string) (models.StoragePartition, error)
	GetStoragePartitionGroupElementByAlias(alias string) (models.StoragePartitionGroup, error)
	UpdateStoragePartition(partition models.StoragePartition) error
	UpdateStoragePartitionGroupElement(partition models.StoragePartitionGroup) error
	GetStoragePartitionsByLocationIdPaginated(pagination models.Pagination) ([]models.StoragePartition, int, error)
	GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error)
	GetStoragePartitionByObjectSignatureAndLocation(signature string, locationId string) (models.StoragePartition, error)
	GetStoragePartitionGroupElementsByStoragePartitionId(partitionGroupId string) ([]models.StoragePartitionGroup, error)
}
