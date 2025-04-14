package repository

import (
	"github.com/ocfl-archive/dlza-manager/models"
)

type ObjectRepository interface {
	GetObjectById(id string) (models.Object, error)
	GetObjectBySignature(signature string) (models.Object, error)
	GetObjectByIdMv(id string) (models.Object, error)
	GetObjectsByChecksum(checksum string) ([]models.Object, error)
	CreateObject(object models.Object) (string, error)
	UpdateObject(object models.Object) error
	GetObjectsByCollectionId(id string) ([]models.Object, error)
	GetObjectsByCollectionIdPaginated(pagination models.Pagination) ([]models.Object, int, error)
	GetResultingQualityForObject(id string) (int, error)
	GetNeededQualityForObject(id string) (int, error)
	GetObjectExceptListOlderThan(collectionId string, ids []string, collectionsNeeded []string) (models.Object, error)
}
