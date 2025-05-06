package repository

import (
	"context"
	"emperror.dev/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RefreshMaterializedViewsRepositoryImpl struct {
	Db *pgxpool.Pool
}

func (r RefreshMaterializedViewsRepositoryImpl) RefreshMaterializedViews() error {
	queryMatColObj := "select refresh_mvw1()"
	_, err := r.Db.Exec(context.Background(), queryMatColObj)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObj)
	}
	queryMatColObjFile := "select refresh_mvw2()"
	_, err = r.Db.Exec(context.Background(), queryMatColObjFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObjFile)
	}
	queryMatTenantFile := "select refresh_mvw3()"
	_, err = r.Db.Exec(context.Background(), queryMatTenantFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatTenantFile)
	}
	return nil
}

func (r RefreshMaterializedViewsRepositoryImpl) RefreshMaterializedViewsFromCollectionToFile() error {

	queryMatColObjFile := "select refresh_mvw2()"
	_, err := r.Db.Exec(context.Background(), queryMatColObjFile)
	if err != nil {
		return errors.Wrapf(err, "Could not RefreshMaterializedViews query: '%s'", queryMatColObjFile)
	}

	return nil
}

func NewRefreshMaterializedViewsRepository(db *pgxpool.Pool) RefreshMaterializedViewsRepository {
	return RefreshMaterializedViewsRepositoryImpl{Db: db}
}
