package repository

import "github.com/ocfl-archive/dlza-manager/models"

type ObjectInstanceCheckRepository interface {
	GetObjectInstanceCheckById(id string) (models.ObjectInstanceCheck, error)
	CreateObjectInstanceCheck(models.ObjectInstanceCheck) (string, error)
	GetObjectInstanceChecksByObjectInstanceId(id string) ([]models.ObjectInstanceCheck, error)
	GetObjectInstanceChecksByObjectInstanceIdPaginated(pagination models.Pagination) ([]models.ObjectInstanceCheck, int, error)
}
