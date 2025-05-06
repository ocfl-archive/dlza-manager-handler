package repository

import "github.com/ocfl-archive/dlza-manager/models"

type CheckerRepository interface {
	GetPathsToCopy() ([]models.CopyPaths, error)
}
