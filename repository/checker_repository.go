package repository

import "github.com/ocfl-archive/dlza-manager-handler/models"

type CheckerRepository interface {
	GetPathsToCopy() ([]models.CopyPaths, error)
	CreatePreparedStatementsForChecker() error
}
