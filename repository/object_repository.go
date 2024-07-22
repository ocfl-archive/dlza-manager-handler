package repository

import (
	"github.com/ocfl-archive/dlza-manager-handler/models"
)

type ObjectRepository interface {
	GetObjectById(id string) (models.Object, error)
	GetObjectsByChecksum(checksum string) ([]models.Object, error)
	CreateObject(object models.Object) (string, error)
	UpdateObject(object models.Object) error
	GetObjectsByCollectionId(id string) ([]models.Object, error)
	GetObjectsByCollectionIdPaginated(pagination models.Pagination) ([]models.Object, int, error)
	CreateObjectPreparedStatements() error
	GetResultingQualityForObject(id string) (int, error)
	GetNeededQualityForObject(id string) (int, error)
}
