package repository

import "github.com/ocfl-archive/dlza-manager/models"

type FileRepository interface {
	CreateFile(file models.File) error
	DeleteFile(id string) error
	GetFileById(id string) (models.File, error)
	GetFilesByObjectIdPaginated(pagination models.Pagination) ([]models.File, int, error)
	GetFilesByCollectionIdPaginated(pagination models.Pagination) ([]models.File, int, error)
	GetMimeTypesForCollectionId(pagination models.Pagination) ([]models.MimeType, int, error)
	GetPronomsForCollectionId(pagination models.Pagination) ([]models.Pronom, int, error)
}
