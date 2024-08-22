package repository

import (
	"github.com/ocfl-archive/dlza-manager-handler/models"
)

type CollectionRepository interface {
	CreateCollection(collection models.Collection) (string, error)
	DeleteCollectionById(id string) error
	GetCollectionsByTenantIdPaginated(pagination models.Pagination) ([]models.Collection, int, error)
	GetCollectionIdByAlias(alias string) (string, error)
	GetCollectionByAlias(alias string) (models.Collection, error)
	UpdateCollection(collection models.Collection) error
	GetCollectionsByTenantId(tenantId string) ([]models.Collection, error)
	GetCollectionById(id string) (models.Collection, error)
	GetCollectionByIdFromMv(id string) (models.Collection, error)
	CreateCollectionPreparedStatements() error
}
