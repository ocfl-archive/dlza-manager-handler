package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"strings"
)

type dispatcherPrepareStmt int

const (
	GetLowQualityCollectionsWithObjectIds dispatcherPrepareStmt = iota
)

func NewDispatcherRepository(db *sql.DB, schema string) DispatcherRepository {
	return &DispatcherRepositoryImpl{Db: db, Schema: schema}
}

type DispatcherRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[dispatcherPrepareStmt]*sql.Stmt
}

func (d *DispatcherRepositoryImpl) CreateDispatcherPreparedStatements() error {
	preparedStatement := map[dispatcherPrepareStmt]string{
		GetLowQualityCollectionsWithObjectIds: strings.Replace("select object_id, alias from %s.quality where ok is false", "%s", d.Schema, -1),
	}
	var err error
	d.PreparedStatement = make(map[dispatcherPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		d.PreparedStatement[key], err = d.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (d *DispatcherRepositoryImpl) GetLowQualityCollectionsWithObjectIds() (map[string][]string, error) {

	rows, err := d.PreparedStatement[GetLowQualityCollectionsWithObjectIds].Query()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get GetLowQualityCollectionsWithObjectIds")
	}
	collectionsWithObjectIds := make(map[string][]string)
	for rows.Next() {
		var objectId string
		var alias string
		err := rows.Scan(&objectId, &alias)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot map low quality collections and objects")
		}
		if len(collectionsWithObjectIds[alias]) != 0 {
			ids := collectionsWithObjectIds[alias]
			ids = append(ids, objectId)
			collectionsWithObjectIds[alias] = ids
		} else {
			collectionsWithObjectIds[alias] = []string{objectId}
		}
	}
	return collectionsWithObjectIds, nil
}
