package repository

import "github.com/ocfl-archive/dlza-manager/models"

type ObjectInstanceRepository interface {
	CreateObjectInstance(models.ObjectInstance) (string, error)
	UpdateObjectInstance(models.ObjectInstance) error
	DeleteObjectInstance(id string) error
	GetObjectInstanceById(id string) (models.ObjectInstance, error)
	GetObjectInstancesByObjectId(id string) ([]models.ObjectInstance, error)
	GetObjectInstancesByObjectIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error)
	GetObjectInstancesByPartitionIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error)
	GetAllObjectInstances() ([]models.ObjectInstance, error)
	GetAmountOfErrorsByCollectionId(id string) (int, error)
	GetObjectInstancesByName(name string) ([]models.ObjectInstance, error)
}
