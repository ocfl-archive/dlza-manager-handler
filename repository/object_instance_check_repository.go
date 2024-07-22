package repository

import "github.com/ocfl-archive/dlza-manager-handler/models"

type ObjectInstanceCheckRepository interface {
	GetObjectInstanceCheckById(id string) (models.ObjectInstanceCheck, error)
	CreateObjectInstanceCheck(models.ObjectInstanceCheck) (string, error)
	GetObjectInstanceChecksByObjectInstanceIdPaginated(pagination models.Pagination) ([]models.ObjectInstanceCheck, int, error)
	CreateObjectInstanceCheckPreparedStatements() error
}
