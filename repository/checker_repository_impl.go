package repository

import (
	"database/sql"
	"fmt"

	"github.com/ocfl-archive/dlza-manager-handler/models"

	"emperror.dev/errors"
)

type CheckerRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[checkerPrepareStmt]*sql.Stmt
}

type checkerPrepareStmt int

const (
	FindAllObjectsWithErrors checkerPrepareStmt = iota
	FindObjectIdByObjectInstance
)

func (c *CheckerRepositoryImpl) CreatePreparedStatementsForChecker() error {

	preparedStatement := map[checkerPrepareStmt]string{
		FindAllObjectsWithErrors: fmt.Sprintf("SELECT oi.object_id, oi.path FROM %s.object_instance oi inner join %s.object_instance_check oic"+
			" on oi.id = oic.object_instance_id"+
			" where checktime <=  CURRENT_DATE - '30 day'::interval"+
			" AND oic.error = 'true'", c.Schema, c.Schema),
		FindObjectIdByObjectInstance: fmt.Sprintf("SELECT oi.status, oi.path FROM %s.object_instance oi where oi.object_id = $1", c.Schema),
	}
	var err error
	c.PreparedStatement = make(map[checkerPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		c.PreparedStatement[key], err = c.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (c *CheckerRepositoryImpl) GetPathsToCopy() ([]models.CopyPaths, error) {

	rowsRs, err := c.PreparedStatement[FindAllObjectsWithErrors].Query()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot execute query")
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
		rowsOI, err := c.PreparedStatement[FindObjectIdByObjectInstance].Query(item)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot execute query")
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

func NewCheckerRepository(db *sql.DB, schema string) CheckerRepository {
	return &CheckerRepositoryImpl{
		Db:     db,
		Schema: schema,
	}
}
