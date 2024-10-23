package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
)

type RefreshMaterializedViewsRepositoryImpl struct {
	Db     *sql.DB
	Schema string
}

func (r RefreshMaterializedViewsRepositoryImpl) RefreshMaterializedViews() error {
	queryMatColObj := fmt.Sprintf("select %s.refresh_mvw1()", r.Schema)
	_, err := r.Db.Exec(queryMatColObj)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObj)
	}
	queryMatColObjFile := fmt.Sprintf("select %s.refresh_mvw2()", r.Schema)
	_, err = r.Db.Exec(queryMatColObjFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObjFile)
	}
	queryMatTenantFile := fmt.Sprintf("select %s.refresh_mvw3()", r.Schema)
	_, err = r.Db.Exec(queryMatTenantFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatTenantFile)
	}
	return nil
}

func (r RefreshMaterializedViewsRepositoryImpl) RefreshMaterializedViewsFromCollectionToFile() error {

	queryMatColObjFile := fmt.Sprintf("select %s.refresh_mvw2()", r.Schema)
	_, err := r.Db.Exec(queryMatColObjFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObjFile)
	}

	return nil
}

func NewRefreshMaterializedViewsRepository(db *sql.DB, schema string) RefreshMaterializedViewsRepository {
	return RefreshMaterializedViewsRepositoryImpl{Db: db, Schema: schema}
}
