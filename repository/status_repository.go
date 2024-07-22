package repository

import "github.com/ocfl-archive/dlza-manager-handler/models"

type StatusRepository interface {
	CreateStatus(collection models.ArchivingStatus) (string, error)
	CheckStatus(id string) (models.ArchivingStatus, error)
	AlterStatus(status models.ArchivingStatus) error
	CreateStatusPreparedStatements() error
}
