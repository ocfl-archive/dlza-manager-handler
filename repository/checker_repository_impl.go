package repository

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ocfl-archive/dlza-manager/models"

	"emperror.dev/errors"
)

type CheckerRepositoryImpl struct {
	Db *pgxpool.Pool
}

const (
	FindAllObjectsWithErrors     = "FindAllObjectsWithErrors"
	FindObjectIdByObjectInstance = "FindObjectIdByObjectInstance"
)

func CreateCheckerPreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		FindAllObjectsWithErrors: "SELECT oi.object_id, oi.path FROM object_instance oi inner join object_instance_check oic" +
			" on oi.id = oic.object_instance_id" +
			" where checktime <=  CURRENT_DATE - '30 day'::interval" +
			" AND oic.error = 'true'",
		FindObjectIdByObjectInstance: "SELECT oi.status, oi.path FROM object_instance oi where oi.object_id = $1",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (c *CheckerRepositoryImpl) GetPathsToCopy() ([]models.CopyPaths, error) {

	rowsRs, err := c.Db.Query(context.Background(), FindAllObjectsWithErrors)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot execute query for method: %v", FindAllObjectsWithErrors)
	}

	listObjectI := make([]string, 0)
	for rowsRs.Next() {

		var objectId string
		var to string

		err := rowsRs.Scan(&objectId, &to)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot map data")
		}
		listObjectI = append(listObjectI, objectId)
	}
	list := make([]models.CopyPaths, 0)
	for index, item := range listObjectI {
		rowsOI, err := c.Db.Query(context.Background(), FindObjectIdByObjectInstance, item)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot execute query for method: %v", FindObjectIdByObjectInstance)
		}
		copyPaths := models.CopyPaths{}
		for rowsOI.Next() {
			var status string
			var path string

			err := rowsOI.Scan(&status, &path)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot map data")
			}
			if status == "error" {
				copyPaths.To = path
			}
			if status == "ok" {
				copyPaths.From = path
			}
			if copyPaths.To != "" && copyPaths.From != "" {
				break
			}
		}
		if index == 200 {
			fmt.Print(index)
		}
		list = append(list, copyPaths)
	}
	return nil, nil
}

func NewCheckerRepository(db *pgxpool.Pool) CheckerRepository {
	return &CheckerRepositoryImpl{
		Db: db,
	}
}
